"""
iot_broker.py - Broker MQTT sobre TCP + UDP

TCP: CONNECT/CONNACK, PUBLISH (QoS 0/1), SUBSCRIBE/SUBACK,
     UNSUBSCRIBE, PINGREQ/PINGRESP, DISCONNECT.
UDP: PUBLISH QoS 0 (sensores — fire and forget, sem handshake).

Ambos escutam na mesma porta (BROKER_PORT). TCP e UDP coexistem
na mesma porta.

Wildcards: + (um nível) e # (zero ou mais níveis).
Mensagens retained: última mensagem por tópico, entregue a novos subscribers.
"""

import socket
import threading
import logging
import os
from collections import defaultdict

HOST       = "0.0.0.0" #Escuta habilitada para todas as interfaces de rede disponíveis
PORT       = int(os.environ.get("BROKER_PORT", "1883")) #Porta 1883 para MQTT, pode ser configurada via variável de ambiente
MAX_LISTEN = 500

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BROKER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("broker")

_lock     = threading.Lock() #Como o servidor cria uma thread para cada cliente, o lock impede que duas threads modifiquem os dicionários globais ao mesmo tempo
_clients  = {}               #Client_id -> {"sock": socket, "subs": set[str]} (ID DO CLIENTE PARA SOCKET E LISTA DE INSCRIÇÕES)
_subs     = defaultdict(set) #Dicionário que relaciona um tópico a um conjunto de IDS DE clientes
_retained = {}               # topic -> payload bytes (última mensagem retida por tópico)


#Codificador MQTT para o campo "remaining length" 
def _enc_rem(n: int) -> bytes:
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

#Decodificador MQTT para o campo "remaining length" 
def _dec_rem(sock: socket.socket):
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

#Garante que o sistema ficará em loop até receber exatamente n bytes do socket, ou retorna None se a conexão for fechada antes disso    
def _read_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


#Implementação do algoritmo de correspondência de tópicos MQTT, considerando os curingas "+" e "#". 
#O padrão é dividido em partes usando "/" como separador, e a função recursiva "match" compara cada 
#parte do padrão com a parte correspondente do tópico. O curinga "+" corresponde a exatamente um nível, 
#enquanto o curinga "#" corresponde a zero ou mais níveis. Se o padrão corresponder ao tópico, a função 
#retorna True; caso contrário, retorna False
def _topic_matches(pattern: str, topic: str) -> bool:
    if pattern == topic:
        return True

    def match(p: list, t: list) -> bool:
        if not p:
            return not t
        if p[0] == "#":
            return True
        if not t:
            return p == ["#"]
        if p[0] in ("+", t[0]):
            return match(p[1:], t[1:])
        return False

    return match(pattern.split("/"), topic.split("/"))


#Função invocada quando o dispositivo publica um dado. Ela vasculha as assinaturas ativas, encontra
#os socketes dos clientes interessados, monta o pacote MQTT e envia os bytes. O dispositivo de origem
#não recebe de volta a mensagem que acabou de enviar
def _route(topic: str, payload: bytes, src_id: str):
    """Forward published message to all matching subscribers (except source)."""
    with _lock:
        targets = []
        for pat, ids in _subs.items():
            if _topic_matches(pat, topic):
                for cid in ids:
                    if cid != src_id and cid in _clients:
                        targets.append((cid, _clients[cid]["sock"]))

    topic_b = topic.encode("utf-8")
    var     = bytes([len(topic_b) >> 8, len(topic_b) & 0xFF]) + topic_b + payload
    pkt     = bytes([0x30]) + _enc_rem(len(var)) + var

    for cid, sock in targets:
        try:
            sock.sendall(pkt)
        except Exception as e:
            log.warning(f"Route to {cid} failed: {e}")


#Realiza a leitura do cabeçalho MQTT e realiza as ações correspondentes para cada tipo de pacote!
def _handle(sock: socket.socket, addr):
    cid = None
    try:
        while True:
            hdr = sock.recv(1)
            if not hdr:
                break
            ptype = (hdr[0] >> 4) & 0x0F
            flags = hdr[0] & 0x0F
            rem   = _dec_rem(sock)
            if rem is None:
                break
            data  = _read_exact(sock, rem) if rem else b""
            if data is None:
                break

            #CONNECT 
            if ptype == 1:
                proto_len = (data[0] << 8) | data[1]
                offset    = 2 + proto_len + 4  # skip name + level + flags + keepalive
                cid_len   = (data[offset] << 8) | data[offset + 1]
                cid       = data[offset + 2: offset + 2 + cid_len].decode("utf-8")
                with _lock:
                    if cid in _clients:
                        try:
                            _clients[cid]["sock"].close()
                        except Exception:
                            pass
                    _clients[cid] = {"sock": sock, "subs": set()}
                sock.sendall(bytes([0x20, 0x02, 0x00, 0x00]))
                log.info(f"CONNECT  {cid}  {addr}")

            #PUBLISH ─
            elif ptype == 3:
                retain  = flags & 0x01
                qos     = (flags >> 1) & 0x03
                tlen    = (data[0] << 8) | data[1]
                topic   = data[2: 2 + tlen].decode("utf-8")
                off     = 2 + tlen
                mid     = 0
                if qos > 0:
                    mid  = (data[off] << 8) | data[off + 1]
                    off += 2
                    sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                payload = data[off:]
                if retain:
                    with _lock:
                        _retained[topic] = payload
                log.debug(f"PUBLISH  {cid} → {topic}  ({len(payload)} B)")
                _route(topic, payload, cid)

            #SUBSCRIBE
            elif ptype == 8:
                mid     = (data[0] << 8) | data[1]
                off     = 2
                granted = []
                while off < len(data):
                    tlen  = (data[off] << 8) | data[off + 1]
                    topic = data[off + 2: off + 2 + tlen].decode("utf-8")
                    qos   = data[off + 2 + tlen]
                    with _lock:
                        _subs[topic].add(cid)
                        _clients[cid]["subs"].add(topic)
                        ret = _retained.get(topic)
                    if ret is not None:
                        tb  = topic.encode("utf-8")
                        var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + ret
                        sock.sendall(bytes([0x30]) + _enc_rem(len(var)) + var)
                    granted.append(qos)
                    off += 3 + tlen
                    log.info(f"SUBSCRIBE {cid} → {topic}")
                sock.sendall(
                    bytes([0x90])
                    + _enc_rem(2 + len(granted))
                    + bytes([mid >> 8, mid & 0xFF])
                    + bytes(granted)
                )

            #UNSUBSCRIBE 
            elif ptype == 10:
                mid = (data[0] << 8) | data[1]
                off = 2
                while off < len(data):
                    tlen  = (data[off] << 8) | data[off + 1]
                    topic = data[off + 2: off + 2 + tlen].decode("utf-8")
                    with _lock:
                        _subs[topic].discard(cid)
                        if cid in _clients:
                            _clients[cid]["subs"].discard(topic)
                    off += 2 + tlen
                sock.sendall(bytes([0xB0, 0x02, mid >> 8, mid & 0xFF]))

            #PINGREQ 
            elif ptype == 12:
                sock.sendall(bytes([0xD0, 0x00]))

            #DISCONNECT 
            elif ptype == 14:
                break

    except Exception as e:
        if cid:
            log.warning(f"Error {cid}: {e}")
    finally: #Garante que, ao desconectar, o socket seja fechado e o cliente limpo da memória ao sair
        if cid:
            with _lock:
                if cid in _clients:
                    for t in list(_clients[cid]["subs"]):
                        _subs[t].discard(cid)
                    del _clients[cid]
        try:
            sock.close()
        except Exception:
            pass
        log.info(f"DISCONNECT {cid or addr}")


# ---------------------------------------------------------------------------
# UDP — recepção de PUBLISH de sensores
# ---------------------------------------------------------------------------
# Sensores fazem handshake TCP (CONNECT/CONNACK) mas enviam dados via UDP.
# O datagrama tem o mesmo formato do MQTT PUBLISH QoS 0:
#   [0x30][remaining_len][topic_len_hi][topic_len_lo][topic][payload]
#
# src_id="" ao chamar _route: sensores não subscrevem nada, então não há
# ninguém a excluir do roteamento.

def _parse_udp(data: bytes):
    """Retorna (topic, payload) de um datagrama MQTT PUBLISH QoS 0, ou (None, None)."""
    if len(data) < 4 or (data[0] >> 4) != 3:
        return None, None
    # Decodificar remaining length (mesmo algoritmo do codec TCP)
    i, mult, val = 1, 1, 0
    for _ in range(4):
        if i >= len(data):
            return None, None
        byte = data[i]; i += 1
        val += (byte & 0x7F) * mult
        if not (byte & 0x80):
            break
        mult <<= 7
    if i + 2 > len(data):
        return None, None
    tlen    = (data[i] << 8) | data[i + 1]; i += 2
    if i + tlen > len(data):
        return None, None
    topic   = data[i: i + tlen].decode("utf-8")
    payload = data[i + tlen:]
    return topic, payload


def _udp_listener():
    """Thread que escuta datagramas UDP e roteia como PUBLISH."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, PORT))
    log.info(f"UDP listener em {HOST}:{PORT}")
    while True:
        try:
            data, addr = sock.recvfrom(65535)
            topic, payload = _parse_udp(data)
            if topic is not None:
                log.debug(f"UDP {addr} → {topic} ({len(payload)} B)")
                _route(topic, payload, "")
        except Exception as e:
            log.warning(f"UDP erro: {e}")


#Ponto de entrada! Iniciamos o servidor UDP rodando eternamente em segundo plano. Esta thread 
#morrerá automaticamente se o programa principal for fechado. O sistema reserva a porta 1883 e
#a configura para enfileirar até 500 conexões TCP
def main():
    threading.Thread(target=_udp_listener, daemon=True).start()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(MAX_LISTEN)
    log.info(f"TCP Broker em {HOST}:{PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=_handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
