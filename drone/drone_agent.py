import socket
import threading
import json
import time
import random
import logging
import os

DRONE_ID        = os.environ.get("DRONE_ID", "drone_1")
BROKER_HOST     = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT     = int(os.environ.get("BROKER_PORT", "1883"))
MISSION_MIN     = int(os.environ.get("MISSION_MIN", "20"))
MISSION_MAX     = int(os.environ.get("MISSION_MAX", "60"))
FAILURE_PROB    = float(os.environ.get("FAILURE_PROB", "0.08"))
STATUS_INTERVAL = int(os.environ.get("STATUS_INTERVAL", "10"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{DRONE_ID}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(DRONE_ID)


def _enc_rem(n):
    # codifica o campo "remaining length" do cabeçalho MQTT em formato de comprimento variável
    # cada byte usa 7 bits para valor e 1 bit para indicar se há mais bytes
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            break
    return bytes(out)


def _dec_rem(sock):
    # lê o "remaining length" do cabeçalho MQTT byte a byte
    # retorna None se a conexão foi encerrada
    mult, val = 1, 0
    for _ in range(4):
        b = sock.recv(1)
        if not b:
            return None
        byte = b[0]
        val += (byte & 0x7F) * mult
        if not (byte & 0x80):
            return val
        mult <<= 7
    return None


def _read_exact(sock, n):
    # lê exatamente n bytes do socket, bloqueando até completar
    # retorna None se a conexão for encerrada antes de ler tudo
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            return None
        buf += c
    return buf


def _topic_matches(pattern, topic):
    # verifica se um tópico MQTT bate com um padrão que pode conter wildcards
    # '+' casa com exatamente um nível, '#' casa com zero ou mais níveis finais
    def m(p, t):
        if not p:
            return not t
        if p[0] == "#":
            return True
        if not t:
            return p == ["#"]
        if p[0] in ("+", t[0]):
            return m(p[1:], t[1:])
        return False
    return pattern == topic or m(pattern.split("/"), topic.split("/"))


class MQTTClient:

    def __init__(self, host, port, client_id):
        self.host      = host           # IP do broker
        self.port      = port           # porta do broker
        self.client_id = client_id      # identificador único do cliente MQTT
        self._sock     = None           # socket TCP da conexão com o broker
        self._lock     = threading.Lock()  # evita que duas threads escrevam no socket ao mesmo tempo
        self._cbs      = {}             # dicionário: tópico → lista de funções callback
        self._mid      = 0              # contador de message IDs para pacotes SUBSCRIBE
        self._alive    = False          # controla o loop da thread leitora

    def connect(self, retries=15, delay=3):
        # tenta estabelecer conexão TCP e realizar o handshake MQTT
        # repete até 15 vezes com intervalo de 3s antes de desistir
        for i in range(1, retries + 1):
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.settimeout(5)
                # timeout de 5s na conexão — evita travar indefinidamente se o broker não responder
                self._sock.connect((self.host, self.port))
                self._sock.settimeout(None)
                # remove o timeout após conectar — socket passa a ser bloqueante normal

                cid = self.client_id.encode()
                var = (b"\x00\x04MQTT\x04\x02\x00\x3c"
                       + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid)
                # monta o payload do pacote CONNECT:
                # \x00\x04MQTT = nome do protocolo, \x04 = versão 3.1.1
                # \x02 = connect flags (clean session), \x00\x3c = keepalive 60s
                self._sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
                # 0x10 = tipo CONNECT

                ack = _read_exact(self._sock, 4)
                # lê o CONNACK de 4 bytes enviado pelo broker em resposta ao CONNECT
                if not ack or ack[0] != 0x20 or ack[3] != 0:
                    raise ConnectionError("CONNACK inválido")
                # 0x20 = tipo CONNACK, ack[3] == 0 significa conexão aceita

                self._alive = True
                threading.Thread(target=self._reader, daemon=True).start()
                # sobe thread em background para receber mensagens continuamente
                log.info(f"Conectado ao broker {self.host}:{self.port}")
                return
            except Exception as e:
                log.warning(f"Conexão {i}/{retries}: {e}")
                time.sleep(delay)
        raise ConnectionError(f"Falha ao conectar ao broker {self.host}:{self.port}")

    def publish(self, topic, payload, qos=0, retain=False):
        # monta e envia um pacote MQTT PUBLISH
        # retain=True instrui o broker a guardar a última mensagem do tópico para novos assinantes
        if isinstance(payload, str):
            payload = payload.encode()
        tb  = topic.encode()
        var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
        # payload do PUBLISH: tamanho do tópico (2 bytes) + tópico + mensagem
        f   = (qos << 1) | (1 if retain else 0)
        # flags do cabeçalho: bits 1-2 = QoS, bit 0 = retain
        pkt = bytes([0x30 | f]) + _enc_rem(len(var)) + var
        with self._lock:
            # lock garante que apenas uma thread escreve no socket por vez
            try:
                self._sock.sendall(pkt)
            except Exception as e:
                log.warning(f"Publish falhou: {e}")

    def subscribe(self, topic, callback):
        # registra um callback local e envia pacote SUBSCRIBE ao broker
        if topic not in self._cbs:
            self._cbs[topic] = []
        self._cbs[topic].append(callback)
        # callback é armazenado localmente e chamado pela thread _reader quando chegar mensagem

        self._mid = (self._mid % 65535) + 1
        mid = self._mid
        # message ID único (1–65535) para identificar este SUBSCRIBE no protocolo

        tb  = topic.encode()
        var = bytes([mid >> 8, mid & 0xFF, len(tb) >> 8, len(tb) & 0xFF]) + tb + b"\x00"
        # payload do SUBSCRIBE: message ID (2 bytes) + tamanho do tópico (2 bytes) + tópico + QoS 0
        pkt = bytes([0x82]) + _enc_rem(len(var)) + var
        # 0x82 = tipo SUBSCRIBE (0x80) com flag obrigatória (0x02)
        with self._lock:
            try:
                self._sock.sendall(pkt)
            except Exception as e:
                log.warning(f"Subscribe falhou: {e}")

    def _reader(self):
        # thread em background que lê continuamente mensagens chegando do broker
        # trata pacotes PUBLISH (tipo 3) e PINGREQ (tipo 12)
        while self._alive:
            try:
                hdr = self._sock.recv(1)
                # lê o primeiro byte do próximo pacote MQTT (cabeçalho fixo)
                if not hdr:
                    break
                ptype = (hdr[0] >> 4) & 0x0F
                # tipo do pacote nos bits 7-4
                flags = hdr[0] & 0x0F
                # flags nos bits 3-0
                rem   = _dec_rem(self._sock)
                if rem is None:
                    break
                data  = _read_exact(self._sock, rem) if rem else b""
                if data is None:
                    break

                if ptype == 3:
                    # PUBLISH — chegou uma mensagem de um tópico assinado
                    qos  = (flags >> 1) & 0x03
                    tlen = (data[0] << 8) | data[1]
                    # tamanho do nome do tópico em 2 bytes
                    top  = data[2:2 + tlen].decode()
                    # nome do tópico
                    off  = 2 + tlen
                    if qos > 0:
                        mid = (data[off] << 8) | data[off + 1]
                        off += 2
                        with self._lock:
                            self._sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                        # envia PUBACK confirmando recebimento (obrigatório para QoS 1)
                    msg = data[off:]
                    # payload da mensagem
                    for pat, cbs in self._cbs.items():
                        if _topic_matches(pat, top):
                            # verifica se o tópico recebido bate com algum padrão assinado
                            for cb in cbs:
                                try:
                                    cb(top, msg)
                                    # chama cada callback registrado para este tópico
                                except Exception as e:
                                    log.warning(f"Callback erro: {e}")

                elif ptype == 12:
                    # PINGREQ — broker verificando se o cliente ainda está vivo
                    with self._lock:
                        self._sock.sendall(bytes([0xD0, 0x00]))
                    # responde com PINGRESP para manter a conexão ativa
            except Exception as e:
                if self._alive:
                    log.warning(f"Leitura erro: {e}")
                break


class DroneAgent:

    def __init__(self):
        self.drone_id        = DRONE_ID                        # identificador único do drone
        self.mqtt            = MQTTClient(BROKER_HOST,         # cliente MQTT conectado ao broker local
                                          BROKER_PORT,
                                          DRONE_ID)
        self.status          = "available"                     # estado inicial: pronto para missões
        self.current_mission = None                            # nenhuma missão ativa no início
        self._lock           = threading.Lock()                # protege status e current_mission contra acesso concorrente

    def start(self):
        # ponto de entrada do drone: conecta ao broker, registra assinaturas e entra em loop de heartbeat
        self.mqtt.connect()

        self.mqtt.subscribe(f"strait/drones/{self.drone_id}/dispatch", self._on_dispatch)
        # escuta ordens de despacho enviadas pelo gerenciador de setor

        self.mqtt.subscribe(f"strait/drones/{self.drone_id}/recall", self._on_recall)
        # escuta ordens de recall enviadas pelo gerenciador ao fim de uma missão

        self._publish_status("available")
        # anuncia disponibilidade com retain=True — gerenciadores conectados depois recebem imediatamente

        log.info(f"Drone {self.drone_id} pronto para missões")

        while True:
            time.sleep(STATUS_INTERVAL)
            # heartbeat: republica o status atual a cada STATUS_INTERVAL segundos (padrão: 10s)
            # mantém o gerenciador informado mesmo na ausência de eventos
            with self._lock:
                s = self.status
                m = self.current_mission
            self._publish_status(s, mission=m)

    def _on_dispatch(self, topic: str, payload: bytes):
        # callback acionado pelo broker quando um gerenciador publica uma ordem de despacho
        # garante que o drone só aceita a missão se estiver disponível
        with self._lock:
            if self.status != "available":
                # segunda linha de defesa contra despacho duplo — o Ricart-Agrawala é a primeira
                log.warning(
                    f"Recebeu despacho mas está {self.status} "
                    f"(missão={self.current_mission}), ignorando"
                )
                return
            self.status          = "busy"
            # muda para busy dentro do lock antes de processar o JSON
            # fecha a janela de race condition entre a verificação e a mudança de estado
            self.current_mission = None

        try:
            msg      = json.loads(payload)
            occ_id   = msg["occurrence_id"]    # identificador da ocorrência a atender
            occ_type = msg["occurrence_type"]  # tipo da ocorrência (ex: "bloqueio_de_rota")
            sector   = msg["sector_id"]        # setor que originou o despacho
        except Exception as e:
            log.warning(f"Despacho inválido: {e}")
            with self._lock:
                self.status = "available"
            # reverte para available se o JSON for malformado — evita drone travado em busy
            return

        with self._lock:
            self.current_mission = occ_id

        log.info(
            f"DESPACHADO para ocorrência {occ_id} "
            f"(tipo={occ_type}, setor={sector})"
        )
        self._publish_status("busy", mission=occ_id)
        # informa ao gerenciador que aceitou o despacho

        threading.Thread(
            target=self._execute_mission,
            args=(msg,),
            daemon=True
        ).start()
        # executa a missão em thread separada para não bloquear o loop de heartbeat

    def _on_recall(self, topic: str, payload: bytes):
        # callback acionado pelo gerenciador ao fim da missão para liberar o drone
        # o recall é incondicional — o drone volta a available independente do que estiver fazendo
        log.info("RECALL recebido, retornando à base")
        with self._lock:
            self.status          = "available"
            self.current_mission = None
        self._publish_status("available")

    def _execute_mission(self, dispatch_msg: dict):
        # simula a execução de uma missão de campo
        # com FAILURE_PROB de chance (8%), a missão falha em algum ponto aleatório
        occ_id   = dispatch_msg["occurrence_id"]
        duration = random.uniform(MISSION_MIN, MISSION_MAX)
        # duração aleatória entre MISSION_MIN (20s) e MISSION_MAX (60s)

        will_fail  = random.random() < FAILURE_PROB
        # sorteia se este drone vai falhar durante a missão
        fail_after = random.uniform(duration * 0.2, duration * 0.8) if will_fail else None
        # se vai falhar, define o momento — sempre entre 20% e 80% da duração
        # evita falha imediata no início ou exatamente no fim

        log.info(
            f"Missão {occ_id}: duração={duration:.0f}s "
            f"{'⚠ FALHA PROGRAMADA em ' + f'{fail_after:.0f}s' if will_fail else ''}"
        )

        elapsed        = 0
        check_interval = 5
        # verifica condição de falha a cada 5s em vez de usar sleep(duration)
        # permite detectar o momento de falha com granularidade de 5s

        while elapsed < duration:
            time.sleep(check_interval)
            elapsed += check_interval

            if will_fail and fail_after and elapsed >= fail_after:
                # momento de falha atingido — simula abate ou perda de conectividade
                log.warning(
                    f"FALHA SIMULADA durante missão {occ_id} "
                    f"(abate / perda de conectividade)"
                )
                self._publish_status("offline")
                # notifica o gerenciador imediatamente — ele vai re-enfileirar a ocorrência
                with self._lock:
                    self.status          = "offline"
                    self.current_mission = None
                time.sleep(random.uniform(30, 60))
                # simula tempo de recuperação: entre 30s e 60s fora de operação
                with self._lock:
                    self.status = "available"
                self._publish_status("available")
                log.info("Recuperado, voltando a disponível")
                return
                # encerra a missão prematuramente — o gerenciador já cuidou da realocação

        # missão concluída normalmente sem falha
        log.info(f"Missão {occ_id} CONCLUÍDA com sucesso")
        with self._lock:
            self.status          = "available"
            self.current_mission = None
        self._publish_status("available")

    def _publish_status(self, status: str, mission=None):
        # serializa e publica o estado atual do drone no tópico de status
        # retain=True garante que gerenciadores conectados depois recebam o último estado sem esperar o heartbeat
        payload = {
            "drone_id":  self.drone_id,
            "status":    status,    # "available", "busy" ou "offline"
            "mission":   mission,   # occurrence_id se busy, None caso contrário
            "timestamp": time.time(),
        }
        self.mqtt.publish(
            f"strait/drones/{self.drone_id}/status",
            json.dumps(payload),
            retain=True
        )
        log.debug(f"Status publicado: {status}")


def main():
    agent = DroneAgent()
    agent.start()


if __name__ == "__main__":
    main()
