"""
drone_agent.py - Agente de Drone Autônomo de Monitoramento

Cada drone:
  - Registra-se no broker do seu setor base com status "available"
  - Aguarda comandos de despacho (qualquer setor pode despachar)
  - Simula execução de missão com duração aleatória
  - Tem probabilidade de falha durante a missão (simula abate/perda de conectividade)
  - Publica atualizações de status periodicamente
"""

import socket
import threading
import json
import time
import random
import logging
import os

DRONE_ID      = os.environ.get("DRONE_ID", "drone_1")
BROKER_HOST   = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT   = int(os.environ.get("BROKER_PORT", "1883"))
MISSION_MIN   = int(os.environ.get("MISSION_MIN", "20"))
MISSION_MAX   = int(os.environ.get("MISSION_MAX", "60"))
FAILURE_PROB  = float(os.environ.get("FAILURE_PROB", "0.08"))  #8% chance de falha
STATUS_INTERVAL = int(os.environ.get("STATUS_INTERVAL", "10"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{DRONE_ID}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(DRONE_ID)


#Funcões de codificação e decodificação das mensagens MQTT, como no broker
def _enc_rem(n):
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
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            return None
        buf += c
    return buf

#Esta função avalia se um tópico publicado se encaixa no padrão de assinatura.
def _topic_matches(pattern, topic):
    def m(p, t):
        if not p:
            return not t
        if p[0] == "#": #multi level
            return True
        if not t:
            return p == ["#"]
        if p[0] in ("+", t[0]): #Single level
            return m(p[1:], t[1:])
        return False
    return pattern == topic or m(pattern.split("/"), topic.split("/"))

#Classe simples que gerencia o ciclo de vida de rede com o broker
class MQTTClient:

    #Método construtor. Inicializa o estado do cliente, as estruturas de dados para os callbacks, 
    #identificador de mensagens, etc
    def __init__(self, host, port, client_id):
        self.host      = host
        self.port      = port
        self.client_id = client_id
        self._sock     = None
        self._lock     = threading.Lock()
        self._cbs      = {}
        self._mid      = 0
        self._alive    = False

    #Abre o socket TCP e envia o pacote CONNECT. Aguarda a resposta CONNACK. Se bem-sucedido, faz
    #o spawn da thread de leitura
    def connect(self, retries=15, delay=3):
        for i in range(1, retries + 1):
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.settimeout(5)
                self._sock.connect((self.host, self.port))
                self._sock.settimeout(None)
                cid = self.client_id.encode()
                var = (b"\x00\x04MQTT\x04\x02\x00\x3c"
                       + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid)
                self._sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
                ack = _read_exact(self._sock, 4)
                if not ack or ack[0] != 0x20 or ack[3] != 0:
                    raise ConnectionError("CONNACK inválido")
                self._alive = True
                threading.Thread(target=self._reader, daemon=True).start()
                log.info(f"Conectado ao broker {self.host}:{self.port}")
                return
            except Exception as e:
                log.warning(f"Conexão {i}/{retries}: {e}")
                time.sleep(delay)
        raise ConnectionError(f"Falha ao conectar ao broker {self.host}:{self.port}")

    def publish(self, topic, payload, qos=0, retain=False):
        if isinstance(payload, str):
            payload = payload.encode()
        tb  = topic.encode()
        var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
        f   = (qos << 1) | (1 if retain else 0)
        pkt = bytes([0x30 | f]) + _enc_rem(len(var)) + var
        with self._lock:
            try:
                self._sock.sendall(pkt)
            except Exception as e:
                log.warning(f"Publish falhou: {e}")

    def subscribe(self, topic, callback):
        if topic not in self._cbs:
            self._cbs[topic] = []
        self._cbs[topic].append(callback)
        self._mid = (self._mid % 65535) + 1
        mid = self._mid
        tb  = topic.encode()
        var = bytes([mid >> 8, mid & 0xFF, len(tb) >> 8, len(tb) & 0xFF]) + tb + b"\x00"
        pkt = bytes([0x82]) + _enc_rem(len(var)) + var
        with self._lock:
            try:
                self._sock.sendall(pkt)
            except Exception as e:
                log.warning(f"Subscribe falhou: {e}")

    def _reader(self):
        while self._alive:
            try:
                hdr = self._sock.recv(1)
                if not hdr:
                    break
                ptype = (hdr[0] >> 4) & 0x0F
                flags = hdr[0] & 0x0F
                rem   = _dec_rem(self._sock)
                if rem is None:
                    break
                data  = _read_exact(self._sock, rem) if rem else b""
                if data is None:
                    break
                if ptype == 3:
                    qos  = (flags >> 1) & 0x03
                    tlen = (data[0] << 8) | data[1]
                    top  = data[2:2 + tlen].decode()
                    off  = 2 + tlen
                    if qos > 0:
                        mid = (data[off] << 8) | data[off + 1]
                        off += 2
                        with self._lock:
                            self._sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                    msg = data[off:]
                    for pat, cbs in self._cbs.items():
                        if _topic_matches(pat, top):
                            for cb in cbs:
                                try:
                                    cb(top, msg)
                                except Exception as e:
                                    log.warning(f"Callback erro: {e}")
                elif ptype == 12:
                    with self._lock:
                        self._sock.sendall(bytes([0xD0, 0x00]))
            except Exception as e:
                if self._alive:
                    log.warning(f"Leitura erro: {e}")
                break


#Entidade drone. Atua como uma FSM autônoma
class DroneAgent:

    #Inicializa a identidade do drone, instancia o MQTTClient, define o estado inicial como disponível
    #e cria um lock dedicado à proteção das variáveis de estado internas do drone!
    def __init__(self):
        self.drone_id       = DRONE_ID
        self.mqtt           = MQTTClient(BROKER_HOST, BROKER_PORT, DRONE_ID)
        self.status         = "available"
        self.current_mission = None
        self._lock          = threading.Lock()


    def start(self):
        #Conecta-se ao broker
        self.mqtt.connect()

        #Assina os tópicos /dispatch e /recall 
        self.mqtt.subscribe(f"strait/drones/{self.drone_id}/dispatch", self._on_dispatch)
        self.mqtt.subscribe(f"strait/drones/{self.drone_id}/recall",   self._on_recall)

        #Inicialmente, publica status "available"
        self._publish_status("available")
        log.info(f"Drone {self.drone_id} pronto para missões")

        #Entra em loop e publica seu status periodicamente
        while True:
            time.sleep(STATUS_INTERVAL)
            with self._lock:
                s  = self.status
                m  = self.current_mission
            self._publish_status(s, mission=m)

    #Callback acionado quando o broker envia uma ordem de missão. Há troca do status para busy caso
    #não esteja alocado para um setor e inicia uma nova thread executando _execute_mission.
    def _on_dispatch(self, topic: str, payload: bytes):
        with self._lock:
            if self.status != "available":
                log.warning(
                    f"Recebeu despacho mas está {self.status} "
                    f"(missão={self.current_mission}), ignorando"
                )
                return
            self.status          = "busy"
            self.current_mission = None  

        try:
            msg      = json.loads(payload)
            occ_id   = msg["occurrence_id"]
            occ_type = msg["occurrence_type"]
            sector   = msg["sector_id"]
        except Exception as e:
            log.warning(f"Despacho inválido: {e}")
            with self._lock:
                self.status = "available"
            return

        with self._lock:
            self.current_mission = occ_id

        log.info(
            f"DESPACHADO para ocorrência {occ_id} "
            f"(tipo={occ_type}, setor={sector})"
        )
        self._publish_status("busy", mission=occ_id)

        threading.Thread(
            target=self._execute_mission,
            args=(msg,),
            daemon=True
        ).start()

    #Callback de interrupção. Se acionado, vai resetar o estado do drone para disponível e limpar
    #a missão atual, abortando a operação
    def _on_recall(self, topic: str, payload: bytes):
        log.info("RECALL recebido, retornando à base")
        with self._lock:
            self.status          = "available"
            self.current_mission = None
        self._publish_status("available")

    #Roda em uma thread isolada. Simula o tempo de uma missão através do sleep. Se o drone falhar, altera o estado
    #para offline e suspende a simulação, tentando se recuperar após um tempo.
    def _execute_mission(self, dispatch_msg: dict):
        occ_id   = dispatch_msg["occurrence_id"]
        duration = random.uniform(MISSION_MIN, MISSION_MAX)

        #Determina ponto de falha aleatório (se aplicável)
        will_fail  = random.random() < FAILURE_PROB
        fail_after = random.uniform(duration * 0.2, duration * 0.8) if will_fail else None

        log.info(
            f"Missão {occ_id}: duração={duration:.0f}s "
            f"{'⚠ FALHA PROGRAMADA em ' + f'{fail_after:.0f}s' if will_fail else ''}"
        )

        elapsed        = 0
        check_interval = 5

        while elapsed < duration:
            time.sleep(check_interval)
            elapsed += check_interval

            if will_fail and fail_after and elapsed >= fail_after:
                log.warning(
                    f"FALHA SIMULADA durante missão {occ_id} "
                    f"(abate / perda de conectividade)"
                )
                self._publish_status("offline")
                with self._lock:
                    self.status          = "offline"
                    self.current_mission = None
                #Após pausa, tenta se recuperar e voltar disponível
                time.sleep(random.uniform(30, 60))
                with self._lock:
                    self.status = "available"
                self._publish_status("available")
                log.info("Recuperado, voltando a disponível")
                return
            
        #Missão concluída com sucesso
        log.info(f"Missão {occ_id} CONCLUÍDA com sucesso")
        with self._lock:
            self.status          = "available"
            self.current_mission = None
        self._publish_status("available")

    #Método que monta a mensagem com a telemetria e o tempo atual
    def _publish_status(self, status: str, mission=None):
        payload = {
            "drone_id":  self.drone_id,
            "status":    status,
            "mission":   mission,
            "timestamp": time.time(),
        }
        self.mqtt.publish(
            f"strait/drones/{self.drone_id}/status",
            json.dumps(payload),
            retain=True #Para que o broker guarde o último estado conhecido do drone.
        )
        log.debug(f"Status publicado: {status}")


#Ponto de entrada
def main():
    agent = DroneAgent()
    agent.start()


if __name__ == "__main__":
    main()
