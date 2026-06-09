import curses
import threading
import json
import time
import socket
import os
from collections import deque
from datetime import datetime

BROKERS = [
    (os.environ.get("BROKER_1_HOST", "broker_1"), int(os.environ.get("BROKER_1_PORT", "1883"))),
    (os.environ.get("BROKER_2_HOST", "broker_2"), int(os.environ.get("BROKER_2_PORT", "1883"))),
    (os.environ.get("BROKER_3_HOST", "broker_3"), int(os.environ.get("BROKER_3_PORT", "1883"))),
    (os.environ.get("BROKER_4_HOST", "broker_4"), int(os.environ.get("BROKER_4_PORT", "1883"))),
]

DRONE_HOME = {
    "drone_a": 1, "drone_b": 1,
    "drone_c": 2, "drone_d": 2,
    "drone_e": 3, "drone_f": 3,
    "drone_g": 4, "drone_h": 4,
}

_lock  = threading.Lock()
_state = {
    "drones": {},
    "sectors": {
        1: {"online": False, "sensors": {}},
        2: {"online": False, "sensors": {}},
        3: {"online": False, "sensors": {}},
        4: {"online": False, "sensors": {}},
    },
    "events": deque(maxlen=10),
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


def _on_message(topic, payload):
    try:
        data = json.loads(payload)
    except Exception:
        return

    parts = topic.split("/")

    with _lock:
        if len(parts) == 4 and parts[1] == "drones" and parts[3] == "status":
            did = parts[2]
            _state["drones"][did] = {
                "status":  data.get("status", "?"),
                "mission": data.get("mission"),
                "ts":      data.get("timestamp", time.time()),
            }

        elif len(parts) == 4 and parts[1] == "sector" and parts[3] == "occurrence":
            _state["events"].appendleft({
                "ts":     time.time(),
                "kind":   "occ",
                "sector": int(parts[2]),
                "id":     data.get("id", "?"),
                "type":   data.get("type", "?"),
                "crit":   data.get("criticality", 0),
                "drone":  None,
            })

        elif len(parts) == 4 and parts[1] == "drones" and parts[3] == "dispatch":
            _state["events"].appendleft({
                "ts":     time.time(),
                "kind":   "dispatch",
                "sector": data.get("sector_id", "?"),
                "id":     data.get("occurrence_id", "?"),
                "type":   data.get("occurrence_type", "?"),
                "crit":   data.get("criticality", 0),
                "drone":  data.get("drone_id", "?"),
            })

        elif len(parts) == 5 and parts[3] == "sensors":
            sn = int(parts[2])
            _state["sectors"][sn]["sensors"][parts[4]] = data


def _mqtt_thread(idx, host, port):
    sector_n  = idx + 1
    client_id = f"monitor_s{sector_n}"

    topics = [
        "strait/drones/+/status",
        "strait/drones/+/dispatch",
        f"strait/sector/{sector_n}/occurrence",
        f"strait/sector/{sector_n}/sensors/+",
    ]

    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.settimeout(None)

            cid = client_id.encode()
            var = b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid
            sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
            ack = _read_exact(sock, 4)
            if not ack or ack[3] != 0:
                raise ConnectionError("CONNACK falhou")

            with _lock:
                _state["sectors"][sector_n]["online"] = True

            for i, topic in enumerate(topics, start=1):
                tb  = topic.encode()
                var = bytes([0, i, len(tb) >> 8, len(tb) & 0xFF]) + tb + b"\x00"
                sock.sendall(bytes([0x82]) + _enc_rem(len(var)) + var)

            while True:
                hdr = sock.recv(1)
                if not hdr:
                    break
                ptype = (hdr[0] >> 4) & 0x0F
                flags = hdr[0] & 0x0F
                rem   = _dec_rem(sock)
                if rem is None:
                    break
                data = _read_exact(sock, rem) if rem else b""
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
                        sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                    _on_message(top, data[off:])
                elif ptype == 12:
                    sock.sendall(bytes([0xD0, 0x00]))

        except Exception:
            pass
        finally:
            with _lock:
                _state["sectors"][sector_n]["online"] = False
            try:
                sock.close()
            except Exception:
                pass

        time.sleep(5)


def _draw(stdscr):
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_GREEN,  -1)
    curses.init_pair(2, curses.COLOR_RED,    -1)
    curses.init_pair(3, curses.COLOR_YELLOW, -1)
    curses.init_pair(4, curses.COLOR_CYAN,   -1)

    GREEN  = curses.color_pair(1)
    RED    = curses.color_pair(2)
    YELLOW = curses.color_pair(3)
    CYAN   = curses.color_pair(4)
    BOLD   = curses.A_BOLD

    curses.curs_set(0)
    stdscr.timeout(400)

    def put(r, c, text, attr=0):
        try:
            stdscr.addstr(r, c, str(text), attr)
        except curses.error:
            pass

    while True:
        if stdscr.getch() == ord('q'):
            break

        stdscr.erase()
        h, w = stdscr.getmaxyx()

        with _lock:
            drones  = dict(_state["drones"])
            sectors = {k: {"online": v["online"], "sensors": dict(v["sensors"])}
                       for k, v in _state["sectors"].items()}
            events  = list(_state["events"])

        r = 0

        title = "MONITORAMENTO DO ESTREITO MARITIMO"
        ts    = datetime.now().strftime("%H:%M:%S")
        put(r, 0, "=" * w, CYAN)
        put(r, max(0, (w - len(title)) // 2), title, CYAN | BOLD)
        put(r, max(0, w - 9), ts, CYAN)
        r += 1

        put(r, 0, " SETORES", BOLD)
        r += 1
        sensor_labels = {1: "radar + boia", 2: "radar + boia", 3: "radar + boia", 4: "radar + boia"}
        for sn in (1, 2, 3, 4):
            ok    = sectors[sn]["online"]
            mark  = "*" if ok else "o"
            color = GREEN if ok else RED
            line  = f"  [S{sn}] {mark} {'ONLINE ' if ok else 'OFFLINE'}  sensores: {sensor_labels[sn]}"
            put(r, 0, line, color | BOLD)
            r += 1
        put(r, 0, "-" * w)
        r += 1

        put(r, 0, " DRONES (8)", BOLD)
        r += 1
        col_w     = w // 2
        drone_ids = [f"drone_{c}" for c in "abcdefgh"]
        for i in range(0, len(drone_ids), 2):
            for col, did in enumerate(drone_ids[i:i+2]):
                info = drones.get(did, {})
                st   = info.get("status", "desconhecido")
                ms   = (info.get("mission") or "")[:16]
                home = DRONE_HOME.get(did, "?")
                if st == "available":
                    color = GREEN
                    lbl   = f"  {did} [S{home}] DISPONIVEL"
                elif st == "busy":
                    color = YELLOW
                    lbl   = f"  {did} [S{home}] MISSAO {ms}"
                elif st == "offline":
                    color = RED
                    lbl   = f"  {did} [S{home}] OFFLINE"
                else:
                    color = 0
                    lbl   = f"  {did} [S{home}] {st.upper()}"
                put(r, col * col_w, lbl[:col_w - 1], color)
            r += 1
        put(r, 0, "-" * w)
        r += 1

        put(r, 0, " ULTIMOS EVENTOS  (occ=ocorrencia detectada  >>>=despachado)", BOLD)
        r += 1
        for ev in events[:7]:
            ts_s  = datetime.fromtimestamp(ev["ts"]).strftime("%H:%M:%S")
            crit  = ev.get("crit", 0)
            occ_t = ev.get("type", "?")[:26]
            sec   = ev.get("sector", "?")
            oid   = ev.get("id", "?")[:18]
            if ev["kind"] == "dispatch":
                drone = ev.get("drone", "?")
                line  = f"  {ts_s} [S{sec}] {oid:<18} {occ_t:<26} crit={crit} >>> {drone}"
                color = YELLOW
            else:
                line  = f"  {ts_s} [S{sec}] {oid:<18} {occ_t:<26} crit={crit}"
                color = RED if crit >= 4 else 0
            put(r, 0, line[:w - 1], color)
            r += 1
        put(r, 0, "-" * w)
        r += 1

        put(r, 0, " SENSORES (ultima leitura)", BOLD)
        r += 1
        sensor_order = [(1, "radar"), (1, "buoy"), (2, "radar"),
                        (2, "buoy"),  (3, "radar"), (3, "buoy"),
                        (4, "radar"), (4, "buoy")]
        for sn, stype in sensor_order:
            sd = sectors[sn]["sensors"].get(stype)
            if not sd:
                continue
            anomaly = sd.get("anomaly", False)
            color   = RED if anomaly else 0
            if stype == "radar":
                line = (f"  radar_s{sn}: {sd.get('vessel_count','?')} emb."
                        f"  {sd.get('avg_speed_kn','?')}kn"
                        f"  {sd.get('bearing_deg','?')}deg")
            else:
                line = (f"  buoy_s{sn}:  ondas={sd.get('wave_height_m','?')}m"
                        f"  corrente={sd.get('current_kn','?')}kn"
                        f"  temp={sd.get('water_temp_c','?')}C")
            if anomaly:
                line += f"  ! {sd.get('alert', '')}"
            put(r, 0, line[:w - 1], color)
            r += 1

        put(h - 1, 0, "=" * w, CYAN)
        put(h - 1, 0, " [q] sair", CYAN)

        stdscr.refresh()


def main():
    for i, (host, port) in enumerate(BROKERS):
        threading.Thread(target=_mqtt_thread, args=(i, host, port), daemon=True).start()

    time.sleep(1.5)

    curses.wrapper(_draw)


if __name__ == "__main__":
    main()
