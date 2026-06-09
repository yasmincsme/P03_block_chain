import socket
import json
import time
import random
import logging
import os

BROKER_IP = os.environ.get("BROKER_HOST", "localhost")
PORT      = int(os.environ.get("BROKER_PORT", "1883"))
SECTOR_ID = os.environ.get("SECTOR_ID", "1")
CLIENT_ID = f"buoy_s{SECTOR_ID}"
TOPIC     = f"strait/sector/{SECTOR_ID}/sensors/buoy"
INTERVAL  = float(os.environ.get("INTERVAL", "6"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [BOIA-S{SECTOR_ID}] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(CLIENT_ID)


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


def build_connect(client_id):
    cid = client_id.encode()
    var = b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid
    return bytes([0x10]) + _enc_rem(len(var)) + var


def build_publish(topic, payload):
    tb  = topic.encode()
    var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
    return bytes([0x30]) + _enc_rem(len(var)) + var


class BuoySensor:
    def __init__(self):
        self.wave_height   = random.uniform(0.3, 1.5)
        self.current_speed = random.uniform(0.2, 1.5)
        self.water_temp    = random.uniform(18, 26)
        self.visibility    = random.uniform(5, 10)

    def read(self):
        self.wave_height   = max(0.1, min(8.0, self.wave_height + random.uniform(-0.3, 0.3)))
        self.current_speed = max(0.0, min(5.0, self.current_speed + random.uniform(-0.2, 0.2)))
        self.water_temp    = max(10, min(32, self.water_temp + random.uniform(-0.5, 0.5)))
        self.visibility    = max(0.1, min(12, self.visibility + random.uniform(-0.8, 0.8)))

        anomaly = False
        alert   = None
        if self.wave_height > 5.0:
            anomaly = True
            alert   = "ondas_criticas"
        elif self.visibility < 1.0:
            anomaly = True
            alert   = "baixa_visibilidade"
        elif self.current_speed > 4.0:
            anomaly = True
            alert   = "corrente_forte"

        return {
            "id":             CLIENT_ID,
            "sector":         SECTOR_ID,
            "wave_height_m":  round(self.wave_height, 2),
            "current_kn":     round(self.current_speed, 2),
            "water_temp_c":   round(self.water_temp, 1),
            "visibility_nmi": round(self.visibility, 1),
            "anomaly":        anomaly,
            "alert":          alert,
            "ts":             int(time.time()),
        }


def run():
    sensor   = BuoySensor()
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    log.info(f"Boia inteligente iniciada | tópico: {TOPIC}")

    while True:
        try:
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.settimeout(5)
            tcp.connect((BROKER_IP, PORT))
            tcp.sendall(build_connect(CLIENT_ID))
            ack = tcp.recv(4)
            tcp.close()

            if ack and len(ack) >= 4 and ack[3] == 0:
                log.info(f"Handshake TCP OK com {BROKER_IP}:{PORT} | publicando via UDP")
                break
            raise ConnectionError("CONNACK inválido")

        except Exception as e:
            log.warning(f"Broker indisponível: {e}, tentando em 5s")
            try:
                tcp.close()
            except Exception:
                pass
            time.sleep(5)

    while True:
        data   = sensor.read()
        packet = build_publish(TOPIC, json.dumps(data).encode())
        udp_sock.sendto(packet, (BROKER_IP, PORT))

        if data["anomaly"]:
            log.warning(f"ANOMALIA: {data['alert']} | ondas={data['wave_height_m']}m vis={data['visibility_nmi']}nmi")
        else:
            log.info(
                f"ondas={data['wave_height_m']}m "
                f"corrente={data['current_kn']}kn "
                f"vis={data['visibility_nmi']}nmi "
                f"temp={data['water_temp_c']}°C"
            )
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()
