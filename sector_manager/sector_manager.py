import socket
import threading
import json
import time
import random
import logging
import os
import heapq

try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
    _WEB3_OK = True
except ImportError:
    _WEB3_OK = False

SECTOR_ID   = int(os.environ.get("SECTOR_ID", "1"))
LOCAL_BROKER = os.environ.get("LOCAL_BROKER", "localhost")
BROKER_PORT  = int(os.environ.get("BROKER_PORT", "1883"))
RA_PORT      = int(os.environ.get("RA_PORT", "5001"))

GETH_URL          = os.environ.get("GETH_URL", "")
SETOR_WALLET_ADDR = os.environ.get("SETOR_WALLET_ADDR", "")
SETOR_WALLET_KEY  = os.environ.get("SETOR_WALLET_KEY", "")
CONTRACT_ADDR     = os.environ.get("CONTRACT_ADDR", "")

PEERS_ENV       = os.environ.get("PEERS", "")
DRONES_ENV      = os.environ.get("DRONES", "drone_1:localhost:1883")
ALL_BROKERS_ENV = os.environ.get("ALL_BROKERS", "localhost:1883")

REPLY_TIMEOUT    = float(os.environ.get("REPLY_TIMEOUT", "6.0"))
MISSION_MIN      = int(os.environ.get("MISSION_MIN", "20"))
MISSION_MAX      = int(os.environ.get("MISSION_MAX", "60"))
OCC_INTERVAL_MIN = int(os.environ.get("OCC_MIN", "25"))
OCC_INTERVAL_MAX = int(os.environ.get("OCC_MAX", "70"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [SETOR-{SECTOR_ID}] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(f"setor_{SECTOR_ID}")

OCCURRENCE_TYPES = {
    "bloqueio_de_rota":       4,
    "embarcacao_a_deriva":    4,
    "risco_ambiental":        4,
    "falha_de_sinalizacao":   3,
    "congestionamento":       3,
    "inspecao_urgente":       3,
    "objeto_nao_identificado":2,
    "inspecao_rotineira":     1,
}


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


def _topic_matches(pattern, topic):
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
        self.host      = host                  #IP do Broker
        self.port      = port                  #Porta do Broker
        self.client_id = client_id             #ID do cliente MQTT
        self._sock     = None                  #Socket para comunicação MQTT
        self._lock     = threading.Lock()      #Evita que duas threads escrevam no socket ao mesmo tempo
        self._cbs      = {}                    #Dicionário: tópico → lista de funções callback
        self._mid      = 0                     #Contador de message IDs para QoS 1
        self._alive    = False                 #Controla o loop da thread leitora

    def connect(self, retries=15, delay=3):
        for i in range(1, retries + 1):
            try:
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #Cria um socket TCP para se conectar ao broker MQTT

                self._sock.settimeout(5)
                #Se não conectar em 5s, lança exceção (evita travar para sempre)

                self._sock.connect((self.host, self.port))
                #Estabelece a conexão TCP com o broker

                self._sock.settimeout(None)
                #Remove o timeout — a partir daqui o socket fica em modo bloqueante normal

                cid = self.client_id.encode()
                var = (b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid)
                #Monta o payload do pacote MQTT CONNECT: (protocol_name, protocol_version, connect_flags, keepalive, client_id)
                
                self._sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
                #Envia o pacote CONNECT:
                #0x10: tipo CONNECT, flags 0

                ack = _read_exact(self._sock, 4)
                #Lê exatamente 4 bytes — o pacote CONNACK do broker

                if not ack or ack[0] != 0x20 or ack[3] != 0:
                    raise ConnectionError("CONNACK inválido")
                # 0x20 = tipo CONNACK
                # ack[3] == 0 significa "conexão aceita"
                # qualquer outro valor = broker recusou

                self._alive = True
                threading.Thread(target=self._reader, daemon=True).start()
                #Ativa a flag e sobe uma thread em background para ficar
                #Lendo mensagens que chegam do broker continuamente

                log.info(f"MQTT conectado {self.host}:{self.port} (id={self.client_id})")
                return  #conexão bem-sucedida, sai do loop
            
            except Exception as e:
                log.warning(f"MQTT conexão {i}/{retries} → {self.host}:{self.port}: {e}")
                time.sleep(delay)

        raise ConnectionError(f"Falha ao conectar ao broker {self.host}:{self.port}")
        #Esgotou as tentativas, lança exceção

    def publish(self, topic, payload, qos=0, retain=False):
        if isinstance(payload, str):
            payload = payload.encode()
            # converte string para bytes se necessário

        tb  = topic.encode()
        var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
        #monta o payload do PUBLISH

        f   = (qos << 1) | (1 if retain else 0)
        #flags do cabeçalho

        pkt = bytes([0x30 | f]) + _enc_rem(len(var)) + var
        #pacote completo

        with self._lock:
            try:
                self._sock.sendall(pkt)
                #adquire lock antes do envio para evitar conflito entre threads
            except Exception as e:
                log.warning(f"Publish falhou {topic}: {e}")

    def subscribe(self, topic, callback):
        if topic not in self._cbs:
            self._cbs[topic] = []
        self._cbs[topic].append(callback)
        #registra a função callback localmente para ser chamada
        #quando chegar uma mensagem nesse tópico

        self._mid = (self._mid % 65535) + 1
        mid = self._mid
        #gera um message ID único (1 a 65535) para identificar este SUBSCRIBE

        tb  = topic.encode()
        var = bytes([mid >> 8, mid & 0xFF,         #message ID em 2 bytes
                     len(tb) >> 8, len(tb) & 0xFF  #tamanho do tópico em 2 bytes
                     ]) + tb + b"\x00"             #tópico + QoS 0
        
        pkt = bytes([0x82]) + _enc_rem(len(var)) + var
        #0x82 = tipo SUBSCRIBE (0x80) com flag obrigatória (0x02)
    
        with self._lock:
            try:
                self._sock.sendall(pkt) #envia o pedido de assinatura ao broker
            except Exception as e:
                log.warning(f"Subscribe falhou {topic}: {e}")

    def _reader(self):
        while self._alive:                  #roda enquanto o cliente estiver ativo
            try:
                hdr = self._sock.recv(1)    #lê 1 byte (o cabeçalho do próximo pacote)
                if not hdr:
                    break                   #broker fechou a conexão

                ptype = (hdr[0] >> 4) & 0x0F    
                #tipo do pacote (bits 7-4)

                flags = hdr[0] & 0x0F
                #flags (bits 3-0)

                rem   = _dec_rem(self._sock)
                #lê o "remaining length" (tamanho do resto)

                if rem is None:
                    break

                data  = _read_exact(self._sock, rem) if rem else b""
                # lê exatamente rem bytes

                if data is None:
                    break

                if ptype == 3:               
                #PUBLISH — chegou uma mensagem

                    qos  = (flags >> 1) & 0x03
                    tlen = (data[0] << 8) | data[1]    #tamanho do tópico
                    top  = data[2:2 + tlen].decode()   #nome do tópico
                    off  = 2 + tlen

                    if qos > 0:
                        mid = (data[off] << 8) | data[off + 1]
                        off += 2
                        with self._lock:
                            self._sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                        #envia PUBACK para confirmar recebimento (QoS 1)

                    msg = data[off:]
                    for pat, cbs in self._cbs.items():
                        if _topic_matches(pat, top):    #verifica se o tópico bate com algum padrão assinado
                            for cb in cbs:
                                try:
                                    cb(top, msg)        #chama cada callback registrado para este tópico
                                except Exception as e:
                                    log.warning(f"Callback erro: {e}")
                
                elif ptype == 12:
                #responde com PINGRESP (0xD0) para manter a conexão viva
                    with self._lock:
                        self._sock.sendall(bytes([0xD0, 0x00]))
            except Exception as e:
                if self._alive:
                    log.warning(f"Leitura MQTT: {e}")
                break


class LamportClock:
    def __init__(self): 
        self._t    = 0       
        #contador do relógio, começa em zero

        self._lock = threading.Lock()
        #lock para evitar condição de corrida entre threads ao ler/escrever o contador

    def tick(self):
    #Chamado quando o gerenciador envia uma mensagem RA (REQUEST ou RELEASE).
        with self._lock:
            self._t += 1
            #incrementa o relógio antes de enviar um evento

            return self._t
            #retorna o timestamp atual para ser enviado na mensagem

    def update(self, received: int):
    #Chamado quando o gerenciador recebe uma mensagem RA de outro setor. É a regra 
    #central do relógio de Lamport: garante a ordenação causal dos eventos entre 
    #processos distribuídos.

        with self._lock:
            self._t = max(self._t, received) + 1
            #recebeu um timestamp do peer — atualiza o relógio local para:
            #o maior entre o valor local e o recebido, mais 1
            #garante que o evento local seja sempre "depois" do evento recebido

            return self._t
        
    @property #Usado quando se quer consultar o timestamp sem gerar um novo evento.
    def value(self):
        with self._lock:
            return self._t


class RicartAgrawala:

    def __init__(self, sector_id: int, peer_count: int,
                 clock: LamportClock, send_fn):
        
        self.sector_id  = sector_id          #ID do gerenciador (1, 2, 3 ou 4)
        self.peer_count = peer_count         #quantos outros gerenciadores existem 
        self.clock      = clock              #relógio de lamport compartilhado
        self.send_fn    = send_fn            #função que envia mensagens TCP aos peers

        self._lock       = threading.Lock()  #protege os dicionários abaixo contra acesso concorrente
        self._requesting = {}
        #drones que ESTE gerenciador está disputando agora
        #drone_id → {ts, crit, occ}

        self._deferred   = {}
        #replies que foram adiados para outros setores
        #drone_id → [lista de sector_ids esperando]

        self._replies    = {}
        #replies já recebidos para cada disputa ativa
        #drone_id → set de sector_ids que já responderam

        self._events     = {}

    def request(self, drone_id: str, criticality: int,
                occurrence_id: str, timeout: float = None) -> bool:
        
        ts = self.clock.tick()
        #gera um novo timestamp lamport para este REQUEST, garantindo que 
        #seja maior do que qualquer evento anterior

        with self._lock:
            self._requesting[drone_id] = {"ts": ts, "crit": criticality, "occ": occurrence_id}
            #registra que este gerenciador está disputando o drone
            
            self._replies[drone_id]    = set()
            #prepara o conjunto de replies - começa vazio 

            ev = threading.Event()
            self._events[drone_id]     = ev
            #cria um evento para bloquear esta thread até receber todos 
            #os replies ou atingir o timeout

        self.send_fn({
            "type":          "REQUEST",
            "drone_id":      drone_id,
            "sector_id":     self.sector_id,
            "timestamp":     ts,
            "criticality":   criticality,
            "occurrence_id": occurrence_id,
        })
        log.info(f"RA REQUEST {drone_id} ts={ts} crit={criticality} occ={occurrence_id}")
        #envia o REQUEST para todos os peers via TCP

        if self.peer_count == 0:
            return True
        #caso especial: sem peers, não precisa esperar ninguém - acesso imediato

        deadline = time.time() + (timeout or REPLY_TIMEOUT)
        #define o prazo máximo de espera para os replies, 
        #baseado no tempo atual + timeout configurado

        while True:
            with self._lock:
                n = len(self._replies.get(drone_id, set()))
            if n >= self.peer_count:
                break
            #recebeu o reply de todos os peers, pode prosseguir

            if time.time() > deadline:
                with self._lock:
                    n = len(self._replies.get(drone_id, set()))
                log.warning(
                    f"RA TIMEOUT {drone_id}: {n}/{self.peer_count} replies, "
                    "assumindo peers falhos como OK"
                )
                break
            #esgotou o prazo, assume que peers sileciosos falharam 
            #e avança mesmo assim 

            time.sleep(0.05)
            #aguarda 50ms antes de verificar novamente 

        log.info(f"RA ADQUIRIDO {drone_id}")
        return True
        #recurso adquirido - pode despachar o drone 

    def handle_request(self, msg: dict):
        sender   = msg["sector_id"]        #quem enviou o REQUEST
        drone_id = msg["drone_id"]         #qual drone está sendo disputado
        req_ts   = msg["timestamp"]        #timestamp lamport do REQUEST recebido
        req_crit = msg["criticality"]      #criticidade da ocorrência do solicitante

        self.clock.update(req_ts)
        #atualiza o relógio local com o timestamp recebido (regra de lamport)

        with self._lock:
            our = self._requesting.get(drone_id)
            #verifica se este gerenciado também está disputando o mesmo drone
            defer = False
            

            if our:
                our_ts, our_crit = our["ts"], our["crit"]

                if our_crit > req_crit:
                #nossa criticidade é maior → temos prioridade → adiamos o reply
                    defer = True
                elif our_crit == req_crit and our_ts < req_ts:
                    defer = True
                #mesma criticidade, mas nosso timestamp é menor (pedimos antes) → adiamos
                elif our_crit == req_crit and our_ts == req_ts and self.sector_id < sender:
                    defer = True
                #tudo igual — desempata pelo ID do setor: menor ID tem prioridade → adiamos

            if defer:
                if drone_id not in self._deferred:
                    self._deferred[drone_id] = []
                if sender not in self._deferred[drone_id]:
                    self._deferred[drone_id].append(sender)
                log.debug(f"RA DEFER reply para setor {sender} ({drone_id})")
                #guarda o setor que pediu para responder depois
            else:
                self._send_reply(drone_id, sender)
                #não estamos disputando ou o peer tem prioridade

    def handle_reply(self, msg: dict):
        drone_id    = msg["drone_id"]
        from_sector = msg["from_sector"]     # quem enviou este REPLY
        to_sector   = msg.get("to_sector")

        if to_sector is not None and to_sector != self.sector_id:
            return
        #ignora se o reply não é para este gerenciador

        with self._lock:
            if drone_id in self._replies:
                self._replies[drone_id].add(from_sector)
                #registra que este peer já concordou

                if len(self._replies[drone_id]) >= self.peer_count:
                    if drone_id in self._events:
                        self._events[drone_id].set()
                #se todos os peers responderam, sinaliza o evento
                #isso desbloqueia a thread que está esperando em request()

        log.debug(f"RA REPLY de setor {from_sector} para {drone_id}")

    def release(self, drone_id: str):
        with self._lock:
            self._requesting.pop(drone_id, None)          #remove da lista de disputas ativas
            self._replies.pop(drone_id, None)             #limpa os replies recebidos
            self._events.pop(drone_id, None)              #remove o evento de sincronização
            deferred = self._deferred.pop(drone_id, [])
            #recupera a lista de setores que estavam aguardando nosso reply

        for sector in deferred:
            self._send_reply(drone_id, sector)
        #agora que liberamos o recurso, enviamos os replies que haviamos adiado

        self.send_fn({
            "type":      "RELEASE",
            "drone_id":  drone_id,
            "sector_id": self.sector_id,
        })
        log.info(f"RA RELEASE {drone_id} ({len(deferred)} replies adiados enviados)")
        #avisa todos os peers que liberamos o drone

    def _send_reply(self, drone_id: str, target_sector: int):
        self.send_fn({
            "type":        "REPLY",
            "drone_id":    drone_id,
            "from_sector": self.sector_id,    #quem está respondendo
            "to_sector":   target_sector,     #para quem vai o reply
        })
        #monta e envia o pacote reply via TCP para o setor solicitante

_SECTOR_ABI = [
    {"name": "recordDispatch",    "type": "function", "stateMutability": "nonpayable", "outputs": [],
     "inputs": [{"name": "sector", "type": "uint8"}, {"name": "occurrenceId", "type": "string"},
                {"name": "droneId", "type": "string"}, {"name": "requestId", "type": "string"}]},
    {"name": "recordRequeue",     "type": "function", "stateMutability": "nonpayable", "outputs": [],
     "inputs": [{"name": "sector", "type": "uint8"}, {"name": "occurrenceId", "type": "string"},
                {"name": "reason", "type": "string"}]},
    {"name": "recordDroneFailed", "type": "function", "stateMutability": "nonpayable", "outputs": [],
     "inputs": [{"name": "sector", "type": "uint8"}, {"name": "occurrenceId", "type": "string"},
                {"name": "droneId", "type": "string"}]},
    {"name": "recordRecall",      "type": "function", "stateMutability": "nonpayable", "outputs": [],
     "inputs": [{"name": "sector", "type": "uint8"}, {"name": "droneId", "type": "string"},
                {"name": "occurrenceId", "type": "string"}]},
]


class BlockchainLogger:
    """Registra eventos operacionais no DroneToken de forma assíncrona (fire-and-forget)."""

    def __init__(self, geth_url: str, wallet_addr: str, wallet_key: str, contract_addr: str):
        self._enabled  = False
        self._contract = None
        self._w3       = None
        self._addr     = wallet_addr
        self._key      = (wallet_key if wallet_key.startswith("0x") else "0x" + wallet_key) if wallet_key else ""
        self._nonce    = None
        self._queue    = []
        self._qlock    = threading.Lock()

        if not (_WEB3_OK and geth_url and wallet_addr and wallet_key and contract_addr):
            log.warning("BlockchainLogger desabilitado — configure GETH_URL/SETOR_WALLET_ADDR/KEY/CONTRACT_ADDR")
            return

        try:
            w3 = Web3(Web3.HTTPProvider(geth_url, request_kwargs={"timeout": 10}))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            self._w3       = w3
            self._contract = w3.eth.contract(
                address=Web3.to_checksum_address(contract_addr),
                abi=_SECTOR_ABI,
            )
            self._enabled = True
            threading.Thread(target=self._worker, daemon=True).start()
            log.info(f"BlockchainLogger conectado a {geth_url} | contrato {contract_addr}")
        except Exception as exc:
            log.warning(f"BlockchainLogger falhou ao inicializar: {exc}")

    def _worker(self):
        while True:
            item = None
            with self._qlock:
                if self._queue:
                    item = self._queue.pop(0)
            if item:
                try:
                    self._send(*item)
                except Exception as exc:
                    log.warning(f"Blockchain tx erro: {exc}")
            else:
                time.sleep(0.2)

    def _send(self, contract_fn, *args):
        addr = Web3.to_checksum_address(self._addr)
        if self._nonce is None:
            self._nonce = self._w3.eth.get_transaction_count(addr)
        tx = contract_fn(*args).build_transaction({
            "from":     addr,
            "nonce":    self._nonce,
            "gas":      200_000,
            "gasPrice": self._w3.eth.gas_price,
        })
        signed  = self._w3.eth.account.sign_transaction(tx, self._key)
        tx_hash = self._w3.eth.send_raw_transaction(signed.rawTransaction)
        self._nonce += 1
        log.info(f"Blockchain {contract_fn.fn_name} → {tx_hash.hex()[:18]}...")

    def _enqueue(self, fn, *args):
        if not self._enabled:
            return
        with self._qlock:
            self._queue.append((fn,) + args)

    def record_dispatch(self, sector: int, occ_id: str, drone_id: str, request_id: str):
        self._enqueue(self._contract.functions.recordDispatch, sector, occ_id, drone_id, request_id)

    def record_requeue(self, sector: int, occ_id: str, reason: str):
        self._enqueue(self._contract.functions.recordRequeue, sector, occ_id, reason)

    def record_drone_failed(self, sector: int, occ_id: str, drone_id: str):
        self._enqueue(self._contract.functions.recordDroneFailed, sector, occ_id, drone_id)

    def record_recall(self, sector: int, drone_id: str, occ_id: str):
        self._enqueue(self._contract.functions.recordRecall, sector, drone_id, occ_id)


class SectorManager:

    def __init__(self):
        self.sector_id = SECTOR_ID

        self.peers = []
        for p in PEERS_ENV.split(","):
            p = p.strip()
            if p:
                h, prt = p.rsplit(":", 1)
                self.peers.append((h, int(prt)))

        self.drone_map = {}
        for entry in DRONES_ENV.split(","):
            entry = entry.strip()
            if entry:
                parts = entry.split(":")
                self.drone_map[parts[0]] = (parts[1], int(parts[2]))

        self.all_brokers = []
        for b in ALL_BROKERS_ENV.split(","):
            b = b.strip()
            if b:
                h, prt = b.rsplit(":", 1)
                self.all_brokers.append((h, int(prt)))

        self.drone_status = {d: "offline" for d in self.drone_map}
        self.drone_lock   = threading.Lock()

        self.occ_queue   = []
        self.occ_counter = 0
        self.occ_lock    = threading.Lock()

        self.missions      = {}
        self.missions_lock = threading.Lock()

        self.clock = LamportClock()

        self.ra = RicartAgrawala(
            sector_id  = self.sector_id,
            peer_count = len(self.peers),
            clock      = self.clock,
            send_fn    = self._broadcast_ra,
        )

        self.ra_conns = {}
        self.ra_lock  = threading.Lock()

        self.blockchain = BlockchainLogger(
            GETH_URL, SETOR_WALLET_ADDR, SETOR_WALLET_KEY, CONTRACT_ADDR
        )

        self.local_mqtt = MQTTClient(
            LOCAL_BROKER, BROKER_PORT,
            f"setor_{SECTOR_ID}_local"
        )
        self.broker_mqtts = {}
        for (h, prt) in self.all_brokers:
            cid = f"setor_{SECTOR_ID}_{h}_{prt}"
            self.broker_mqtts[(h, prt)] = MQTTClient(h, prt, cid)

        self._running = False

    def start(self):
        self._running = True

        threading.Thread(target=self._ra_server, daemon=True).start()
        time.sleep(0.3)

        self.local_mqtt.connect()
        for key, client in self.broker_mqtts.items():
            try:
                client.connect()
            except Exception as e:
                log.warning(f"Broker {key} indisponível: {e}")

        for client in self.broker_mqtts.values():
            client.subscribe("strait/drones/+/status", self._on_drone_status)

        self.local_mqtt.subscribe(
            f"strait/sector/{self.sector_id}/manual_request",
            self._on_manual_request
        )

        time.sleep(2)
        threading.Thread(target=self._connect_ra_peers, daemon=True).start()

        threading.Thread(target=self._occurrence_dispatcher, daemon=True).start()

        log.info(
            f"Setor {self.sector_id} iniciado | "
            f"Drones: {list(self.drone_map.keys())} | "
            f"Peers RA: {self.peers}"
        )

        while self._running:
            time.sleep(1)

    def _ra_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", RA_PORT))
        srv.listen(20)
        log.info(f"RA server escutando na porta {RA_PORT}")
        while self._running:
            try:
                conn, addr = srv.accept()
                threading.Thread(
                    target=self._handle_ra_conn,
                    args=(conn, addr),
                    daemon=True
                ).start()
            except Exception as e:
                log.warning(f"RA server erro: {e}")

    def _handle_ra_conn(self, conn, addr):
        buf = b""
        while self._running:
            try:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    try:
                        msg = json.loads(line.decode())
                        self._process_ra_msg(msg)
                    except Exception as e:
                        log.warning(f"Mensagem RA inválida: {e}")
            except Exception as e:
                log.warning(f"RA conn {addr}: {e}")
                break
        try:
            conn.close()
        except Exception:
            pass

    def _connect_ra_peers(self):
        for (host, port) in self.peers:
            for attempt in range(12):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    sock.connect((host, port))
                    sock.settimeout(None)
                    with self.ra_lock:
                        self.ra_conns[(host, port)] = sock
                    log.info(f"RA conectado ao peer {host}:{port}")
                    break
                except Exception as e:
                    log.warning(f"RA peer {host}:{port} tentativa {attempt + 1}/12: {e}")
                    time.sleep(3)

    def _broadcast_ra(self, msg: dict):
        data = (json.dumps(msg) + "\n").encode()
        with self.ra_lock:
            conns = list(self.ra_conns.values())
        for conn in conns:
            try:
                conn.sendall(data)
            except Exception as e:
                log.warning(f"RA broadcast falhou: {e}")

    def _process_ra_msg(self, msg: dict):
        mtype = msg.get("type")
        if mtype == "REQUEST":
            self.ra.handle_request(msg)
        elif mtype == "REPLY":
            self.ra.handle_reply(msg)
        elif mtype == "RELEASE":
            pass

    def _on_drone_status(self, topic, payload):
        try:
            data     = json.loads(payload)
            parts    = topic.split("/")
            drone_id = parts[2]
            status   = data.get("status", "offline")
            with self.drone_lock:
                if drone_id in self.drone_status:
                    self.drone_status[drone_id] = status
        except Exception as e:
            log.warning(f"Status de drone inválido: {e}")

    def _on_manual_request(self, _topic, payload):
        try:
            data       = json.loads(payload)
            occ_type   = data.get("type")
            request_id = data.get("request_id", "")
            if not occ_type or occ_type not in OCCURRENCE_TYPES:
                log.warning(f"manual_request com tipo desconhecido: '{occ_type}'")
                return
            log.info(f"Solicitação de cliente recebida: {occ_type} (req={request_id[:16] if request_id else '-'})")
            self._enqueue_occurrence(occ_type, "solicitacao_cliente", request_id)
        except Exception as e:
            log.warning(f"manual_request inválido: {e}")

    def _enqueue_occurrence(self, occ_type: str, reason: str, request_id: str = ""):
        criticality = OCCURRENCE_TYPES.get(occ_type, 1)
        ts          = self.clock.tick()

        with self.occ_lock:
            self.occ_counter += 1
            occ_id = f"occ_s{self.sector_id}_{self.occ_counter:04d}"
            occ = {
                "id":          occ_id,
                "type":        occ_type,
                "criticality": criticality,
                "sector_id":   self.sector_id,
                "timestamp":   ts,
                "reason":      reason,
                "request_id":  request_id,
            }
            heapq.heappush(
                self.occ_queue,
                (-criticality, ts, self.sector_id, self.occ_counter, occ)
            )

        log.info(
            f"OCORRÊNCIA enfileirada: {occ_id} "
            f"tipo={occ_type} crit={criticality}"
        )
        self.local_mqtt.publish(
            f"strait/sector/{self.sector_id}/occurrence",
            json.dumps(occ)
        )

    def _occurrence_dispatcher(self):
        while self._running:
            occ = None
            with self.occ_lock:
                if self.occ_queue:
                    _, _, _, _, occ = heapq.heappop(self.occ_queue)

            if occ:
                threading.Thread(
                    target=self._handle_occurrence,
                    args=(occ,),
                    daemon=True
                ).start()
            else:
                time.sleep(0.5)

    def _handle_occurrence(self, occ: dict):
        occ_id = occ["id"]
        crit   = occ["criticality"]
        log.info(f"Tratando {occ_id} (tipo={occ['type']}, crit={crit})")

        max_attempts = 15
        drone_id     = None

        for attempt in range(1, max_attempts + 1):
            candidate = self._pick_available_drone()
            if not candidate:
                log.info(f"{occ_id}: nenhum drone disponível (tentativa {attempt}), aguardando...")
                time.sleep(5)
                continue

            drone_id = candidate
            log.info(f"{occ_id}: tentando adquirir {drone_id} via Ricart-Agrawala (tentativa {attempt})")

            with self.drone_lock:
                if self.drone_status.get(drone_id) != "available":
                    drone_id = None
                    continue
                self.drone_status[drone_id] = "requesting"

            self.ra.request(drone_id, crit, occ_id)

            with self.drone_lock:
                current = self.drone_status.get(drone_id)
                if current in ("available", "requesting"):
                    self.drone_status[drone_id] = "busy"
                    break
                else:
                    log.warning(
                        f"{occ_id}: {drone_id} ficou {current} durante RA, liberando e tentando outro"
                    )
                    self.ra.release(drone_id)
                    drone_id = None

        if not drone_id:
            log.error(f"{occ_id}: FALHA ao adquirir drone após {max_attempts} tentativas")
            reason = f"sem-drone-disponivel apos {max_attempts} tentativas"
            self.blockchain.record_requeue(self.sector_id, occ_id, reason)
            time.sleep(10)
            self._enqueue_occurrence(occ["type"], f"re-enfileirado: {occ['reason']}", occ.get("request_id", ""))
            return

        self._dispatch_drone(drone_id, occ)

    def _pick_available_drone(self):
        with self.drone_lock:
            available = [d for d, s in self.drone_status.items() if s == "available"]
        return random.choice(available) if available else None

    def _dispatch_drone(self, drone_id: str, occ: dict):
        occ_id = occ["id"]

        with self.missions_lock:
            self.missions[drone_id] = occ_id

        dispatch_msg = {
            "drone_id":        drone_id,
            "sector_id":       self.sector_id,
            "occurrence_id":   occ_id,
            "occurrence_type": occ["type"],
            "criticality":     occ["criticality"],
            "timestamp":       time.time(),
        }

        log.info(f"DESPACHANDO {drone_id} → {occ_id} (tipo={occ['type']})")
        self.blockchain.record_dispatch(
            self.sector_id, occ_id, drone_id, occ.get("request_id", "")
        )

        broker_addr = self.drone_map.get(drone_id)
        if broker_addr and broker_addr in self.broker_mqtts:
            self.broker_mqtts[broker_addr].publish(
                f"strait/drones/{drone_id}/dispatch",
                json.dumps(dispatch_msg)
            )
        else:
            self.local_mqtt.publish(
                f"strait/drones/{drone_id}/dispatch",
                json.dumps(dispatch_msg)
            )

        mission_duration = random.uniform(MISSION_MIN, MISSION_MAX)
        log.info(f"Missão {occ_id}: duração estimada {mission_duration:.0f}s")

        elapsed        = 0
        check_interval = 5
        reallocated    = False

        while elapsed < mission_duration:
            time.sleep(check_interval)
            elapsed += check_interval

            with self.drone_lock:
                status = self.drone_status.get(drone_id, "offline")

            if status == "offline":
                log.warning(f"{occ_id}: drone {drone_id} FALHOU em missão, realocando")
                self.blockchain.record_drone_failed(self.sector_id, occ_id, drone_id)
                with self.missions_lock:
                    self.missions.pop(drone_id, None)
                self.ra.release(drone_id)
                reason = f"realocacao apos falha de {drone_id}"
                self.blockchain.record_requeue(self.sector_id, occ_id, reason)
                self._enqueue_occurrence(occ["type"], reason, occ.get("request_id", ""))
                reallocated = True
                return

        if not reallocated:
            log.info(f"Missão {occ_id} concluída, liberando {drone_id}")
            self.blockchain.record_recall(self.sector_id, drone_id, occ_id)

            with self.drone_lock:
                if self.drone_status.get(drone_id) == "busy":
                    self.drone_status[drone_id] = "available"

            with self.missions_lock:
                self.missions.pop(drone_id, None)

            self.ra.release(drone_id)

            recall_msg = {
                "drone_id":  drone_id,
                "command":   "recall",
                "sector_id": self.sector_id,
            }
            if broker_addr and broker_addr in self.broker_mqtts:
                self.broker_mqtts[broker_addr].publish(
                    f"strait/drones/{drone_id}/recall",
                    json.dumps(recall_msg)
                )


def main():
    manager = SectorManager()
    manager.start()


if __name__ == "__main__":
    main()
