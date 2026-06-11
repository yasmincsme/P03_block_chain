import json
import logging
import os
import socket
import sys
import threading
import time

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "3001"))
CLIENT_ID   = os.environ.get("CLIENT_ID", "client_terminal")

OCCURRENCE_TYPES = [
    ("bloqueio_de_rota",         4),
    ("embarcacao_a_deriva",      4),
    ("risco_ambiental",          4),
    ("falha_de_sinalizacao",     3),
    ("congestionamento",         3),
    ("inspecao_urgente",         3),
    ("objeto_nao_identificado",  2),
    ("inspecao_rotineira",       1),
]

logging.basicConfig(level=logging.WARNING, format="%(message)s")
log = logging.getLogger("client")


# ─── MQTT helpers ─────────────────────────────────────────────────────────────

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


# ─── MQTTClient ───────────────────────────────────────────────────────────────

class MQTTClient:

    def __init__(self, host, port, client_id):
        self.host      = host
        self.port      = port
        self.client_id = client_id
        self._sock     = None
        self._lock     = threading.Lock()
        self._cbs      = {}
        self._mid      = 0
        self._alive    = False

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
                return
            except Exception as e:
                print(f"  Tentativa {i}/{retries}: {e}")
                time.sleep(delay)
        raise ConnectionError(f"Não foi possível conectar ao broker {self.host}:{self.port}")

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
                data = _read_exact(self._sock, rem) if rem else b""
                if data is None:
                    break

                if ptype == 3:
                    # PUBLISH recebido
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


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 52)
    print("   CLIENTE DE SOLICITAÇÃO DE DRONE")
    print("=" * 52)
    print(f"\n  Broker : {BROKER_HOST}:{BROKER_PORT}")
    print(f"  ID     : {CLIENT_ID}")
    print("\n  Conectando...", end=" ", flush=True)

    client = MQTTClient(BROKER_HOST, BROKER_PORT, CLIENT_ID)
    try:
        client.connect()
    except ConnectionError as e:
        print(f"FALHA\n  {e}")
        sys.exit(1)
    print("OK")

    occ_menu = "\nTipos de ocorrência:\n"
    for i, (name, crit) in enumerate(OCCURRENCE_TYPES, 1):
        occ_menu += f"  {i}. {name:<32} criticidade={crit}\n"

    while True:
        print(f"\n{'─' * 52}")
        try:
            raw = input("Setor [1-4] (ou 'q' para sair): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando.")
            sys.exit(0)

        if raw.lower() == "q":
            print("Encerrando.")
            sys.exit(0)

        if raw not in ("1", "2", "3", "4"):
            print("  Setor inválido.")
            continue

        sector = int(raw)
        print(occ_menu)

        try:
            raw2 = input(f"Ocorrência [1-{len(OCCURRENCE_TYPES)}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando.")
            sys.exit(0)

        if not raw2.isdigit() or not (1 <= int(raw2) <= len(OCCURRENCE_TYPES)):
            print("  Opção inválida.")
            continue

        occ_type, criticality = OCCURRENCE_TYPES[int(raw2) - 1]
        payload = json.dumps({"type": occ_type, "criticality": criticality})
        client.publish(f"strait/sector/{sector}/manual_request", payload)
        print(f"\n  Enviado → setor {sector} | {occ_type} (crit={criticality})")


if __name__ == "__main__":
    main()
