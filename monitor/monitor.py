import curses
import threading
import json
import time
import socket
import os
from collections import deque
from datetime import datetime

try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
    _WEB3 = True
except ImportError:
    _WEB3 = False

BROKERS = [
    (os.environ.get("BROKER_1_HOST", "broker_1"), int(os.environ.get("BROKER_1_PORT", "1883"))),
    (os.environ.get("BROKER_2_HOST", "broker_2"), int(os.environ.get("BROKER_2_PORT", "1883"))),
    (os.environ.get("BROKER_3_HOST", "broker_3"), int(os.environ.get("BROKER_3_PORT", "1883"))),
    (os.environ.get("BROKER_4_HOST", "broker_4"), int(os.environ.get("BROKER_4_PORT", "1883"))),
]

GETH_URL       = os.environ.get("GETH_URL",       "")
CONTRACT_ADDR  = os.environ.get("CONTRACT_ADDR",   "")
EMPRESA_A_ADDR = os.environ.get("EMPRESA_A_ADDR",  "")
EMPRESA_B_ADDR = os.environ.get("EMPRESA_B_ADDR",  "")

DRONE_HOME = {
    "drone_a": 1, "drone_b": 1,
    "drone_c": 2, "drone_d": 2,
    "drone_e": 3, "drone_f": 3,
    "drone_g": 4, "drone_h": 4,
}

_CHAIN_ABI = [
    {"name": "balances", "type": "function", "stateMutability": "view",
     "outputs": [{"type": "uint256"}], "inputs": [{"name": "", "type": "address"}]},
    {"name": "Transfer", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "from",   "type": "address", "indexed": True},
         {"name": "to",     "type": "address", "indexed": True},
         {"name": "amount", "type": "uint256", "indexed": False},
     ]},
    {"name": "DroneRequested", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "requester",      "type": "address", "indexed": True},
         {"name": "sector",         "type": "uint8",   "indexed": True},
         {"name": "occurrenceType", "type": "string",  "indexed": False},
         {"name": "criticality",    "type": "uint8",   "indexed": False},
         {"name": "cost",           "type": "uint256", "indexed": False},
         {"name": "requestId",      "type": "string",  "indexed": False},
         {"name": "ts",             "type": "uint256", "indexed": False},
     ]},
    {"name": "DroneDispatched", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",       "type": "uint8",  "indexed": True},
         {"name": "occurrenceId", "type": "string", "indexed": False},
         {"name": "droneId",      "type": "string", "indexed": False},
         {"name": "requestId",    "type": "string", "indexed": False},
         {"name": "ts",           "type": "uint256","indexed": False},
     ]},
    {"name": "OccurrenceRequeued", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",       "type": "uint8",  "indexed": True},
         {"name": "occurrenceId", "type": "string", "indexed": False},
         {"name": "reason",       "type": "string", "indexed": False},
         {"name": "ts",           "type": "uint256","indexed": False},
     ]},
    {"name": "DroneFailed", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",       "type": "uint8",  "indexed": True},
         {"name": "occurrenceId", "type": "string", "indexed": False},
         {"name": "droneId",      "type": "string", "indexed": False},
         {"name": "ts",           "type": "uint256","indexed": False},
     ]},
    {"name": "DroneRecalled", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",       "type": "uint8",  "indexed": True},
         {"name": "droneId",      "type": "string", "indexed": False},
         {"name": "occurrenceId", "type": "string", "indexed": False},
         {"name": "ts",           "type": "uint256","indexed": False},
     ]},
]

_lock  = threading.Lock()
_state = {
    "drones": {},
    "sectors": {
        1: {"broker": False, "last_msg": 0},
        2: {"broker": False, "last_msg": 0},
        3: {"broker": False, "last_msg": 0},
        4: {"broker": False, "last_msg": 0},
    },
    "events": deque(maxlen=10),
    "chain": {
        "block": None,
        "bal_a": None,
        "bal_b": None,
        "txs":   deque(maxlen=6),
        "ok":    False,
    },
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


def _on_message(topic, payload, sector_n):
    try:
        data = json.loads(payload)
    except Exception:
        return

    parts = topic.split("/")
    now   = time.time()

    with _lock:
        _state["sectors"][sector_n]["last_msg"] = now

        if len(parts) == 4 and parts[1] == "drones" and parts[3] == "status":
            did = parts[2]
            _state["drones"][did] = {
                "status":  data.get("status", "?"),
                "mission": data.get("mission"),
                "ts":      data.get("timestamp", now),
            }

        elif len(parts) == 4 and parts[1] == "sector" and parts[3] == "occurrence":
            _state["events"].appendleft({
                "ts":     now,
                "kind":   "occ",
                "sector": int(parts[2]),
                "id":     data.get("id", "?"),
                "type":   data.get("type", "?"),
                "crit":   data.get("criticality", 0),
                "drone":  None,
            })

        elif len(parts) == 4 and parts[1] == "drones" and parts[3] == "dispatch":
            _state["events"].appendleft({
                "ts":     now,
                "kind":   "dispatch",
                "sector": data.get("sector_id", "?"),
                "id":     data.get("occurrence_id", "?"),
                "type":   data.get("occurrence_type", "?"),
                "crit":   data.get("criticality", 0),
                "drone":  data.get("drone_id", "?"),
            })


def _mqtt_thread(idx, host, port):
    sector_n  = idx + 1
    client_id = f"monitor_s{sector_n}"

    topics = [
        "strait/drones/+/status",
        "strait/drones/+/dispatch",
        f"strait/sector/{sector_n}/occurrence",
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
                _state["sectors"][sector_n]["broker"] = True

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
                    _on_message(top, data[off:], sector_n)
                elif ptype == 12:
                    sock.sendall(bytes([0xD0, 0x00]))

        except Exception:
            pass
        finally:
            with _lock:
                _state["sectors"][sector_n]["broker"] = False
            try:
                sock.close()
            except Exception:
                pass

        time.sleep(5)


def _blockchain_thread():
    if not (_WEB3 and GETH_URL and CONTRACT_ADDR):
        return

    w3 = Web3(Web3.HTTPProvider(GETH_URL, request_kwargs={"timeout": 5}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(CONTRACT_ADDR),
        abi=_CHAIN_ABI,
    )

    T = {
        "Transfer":          Web3.keccak(text="Transfer(address,address,uint256)").hex(),
        "DroneRequested":    Web3.keccak(text="DroneRequested(address,uint8,string,uint8,uint256,string,uint256)").hex(),
        "DroneDispatched":   Web3.keccak(text="DroneDispatched(uint8,string,string,string,uint256)").hex(),
        "OccurrenceRequeued":Web3.keccak(text="OccurrenceRequeued(uint8,string,string,uint256)").hex(),
        "DroneFailed":       Web3.keccak(text="DroneFailed(uint8,string,string,uint256)").hex(),
        "DroneRecalled":     Web3.keccak(text="DroneRecalled(uint8,string,string,uint256)").hex(),
    }
    T_BY_HASH = {v: k for k, v in T.items()}
    ZERO_ADDR = "0x" + "0" * 40

    last_scanned = 0

    while True:
        try:
            blk = w3.eth.block_number

            bal_a = bal_b = None
            if EMPRESA_A_ADDR:
                try:
                    bal_a = contract.functions.balances(
                        Web3.to_checksum_address(EMPRESA_A_ADDR)).call()
                except Exception:
                    pass
            if EMPRESA_B_ADDR:
                try:
                    bal_b = contract.functions.balances(
                        Web3.to_checksum_address(EMPRESA_B_ADDR)).call()
                except Exception:
                    pass

            from_blk = max(last_scanned + 1, blk - 200) if last_scanned else max(0, blk - 200)
            if blk >= from_blk:
                try:
                    logs = w3.eth.get_logs({
                        "fromBlock": from_blk,
                        "toBlock":   blk,
                        "address":   contract.address,
                    })
                    for raw in sorted(logs, key=lambda x: (x["blockNumber"], x["transactionIndex"])):
                        t0       = raw["topics"][0].hex() if raw["topics"] else ""
                        ev_name  = T_BY_HASH.get(t0)
                        if not ev_name:
                            continue
                        try:
                            ev = getattr(contract.events, ev_name)().process_log(raw)
                            a  = ev["args"]
                            if ev_name == "Transfer":
                                frm = a["from"]
                                if frm == ZERO_ADDR:
                                    desc = f"MINT     +{a['amount']} tok → {a['to'][:10]}..."
                                else:
                                    desc = f"PAGAMENT -{a['amount']} tok  {frm[:10]}..."
                            elif ev_name == "DroneRequested":
                                desc = (f"REQ S{a['sector']} crit={a['criticality']}"
                                        f" -{a['cost']}tok {a['occurrenceType'][:12]}")
                            elif ev_name == "DroneDispatched":
                                desc = f"DESPACHO S{a['sector']} {a['droneId']} → {a['occurrenceId'][:12]}"
                            elif ev_name == "OccurrenceRequeued":
                                desc = f"REQUEUE  S{a['sector']} {a['occurrenceId'][:12]} ({a['reason'][:14]})"
                            elif ev_name == "DroneFailed":
                                desc = f"FALHA    S{a['sector']} {a['droneId']} occ={a['occurrenceId'][:10]}"
                            elif ev_name == "DroneRecalled":
                                desc = f"RETORNO  S{a['sector']} {a['droneId']} occ={a['occurrenceId'][:10]}"
                            with _lock:
                                _state["chain"]["txs"].appendleft({
                                    "blk":  raw["blockNumber"],
                                    "ev":   ev_name,
                                    "desc": desc,
                                })
                        except Exception:
                            pass
                except Exception:
                    pass
                last_scanned = blk

            with _lock:
                _state["chain"].update({"block": blk, "bal_a": bal_a, "bal_b": bal_b, "ok": True})

        except Exception:
            with _lock:
                _state["chain"]["ok"] = False

        time.sleep(10)


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

        now = time.time()
        with _lock:
            drones    = dict(_state["drones"])
            sectors   = {
                k: {
                    "broker":   v["broker"],
                    "active":   v["broker"] and (now - v["last_msg"]) < 60,
                    "last_msg": v["last_msg"],
                }
                for k, v in _state["sectors"].items()
            }
            events    = list(_state["events"])
            chain     = {k: v for k, v in _state["chain"].items() if k != "txs"}
            chain_txs = list(_state["chain"]["txs"])

        r = 0

        title = "MONITORAMENTO DO ESTREITO MARITIMO"
        ts    = datetime.now().strftime("%H:%M:%S")
        put(r, 0, "=" * w, CYAN)
        put(r, max(0, (w - len(title)) // 2), title, CYAN | BOLD)
        put(r, max(0, w - 9), ts, CYAN)
        r += 1

        put(r, 0, " SETORES", BOLD)
        r += 1
        for sn in (1, 2, 3, 4):
            broker = sectors[sn]["broker"]
            active = sectors[sn]["active"]
            if active:
                color, mark, label = GREEN,  "*", "ATIVO  "
            elif broker:
                color, mark, label = YELLOW, "-", "AGUARD."
            else:
                color, mark, label = RED,    "o", "OFFLINE"
            put(r, 0, f"  [S{sn}] {mark} {label}", color | BOLD)
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

        put(r, 0, " ULTIMOS EVENTOS  (occ=ocorrencia  >>>=drone despachado)", BOLD)
        r += 1
        for ev in events[:5]:
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

        if GETH_URL:
            ok_chain = chain.get("ok", False)
            blk_str  = f"#{chain['block']}" if chain.get("block") is not None else "---"
            conn_str = "● conectado" if ok_chain else "○ desconectado"
            put(r, 0, f" BLOCKCHAIN  bloco {blk_str}  {conn_str}",
                BOLD | (CYAN if ok_chain else RED))
            r += 1

            bal_a = chain.get("bal_a")
            bal_b = chain.get("bal_b")
            a_str = f"{bal_a} tok" if bal_a is not None else "---"
            b_str = f"{bal_b} tok" if bal_b is not None else "---"
            put(r, 0, f"  saldo  empresa_a: {a_str:<10}  empresa_b: {b_str}")
            r += 1

            EV_COLOR = {
                "Transfer":           CYAN,
                "DroneRequested":     YELLOW,
                "DroneDispatched":    GREEN,
                "OccurrenceRequeued": YELLOW,
                "DroneFailed":        RED,
                "DroneRecalled":      0,
            }
            for tx in chain_txs[:5]:
                color = EV_COLOR.get(tx.get("ev", ""), 0)
                put(r, 0, f"  #{tx['blk']:<6} {tx['desc']}"[:w - 1], color)
                r += 1

            put(r, 0, "-" * w)
            r += 1

        put(h - 1, 0, "=" * w, CYAN)
        put(h - 1, 0, " [q] sair", CYAN)

        stdscr.refresh()


def main():
    for i, (host, port) in enumerate(BROKERS):
        threading.Thread(target=_mqtt_thread, args=(i, host, port), daemon=True).start()

    if _WEB3 and GETH_URL and CONTRACT_ADDR:
        threading.Thread(target=_blockchain_thread, daemon=True).start()

    time.sleep(1.5)

    curses.wrapper(_draw)


if __name__ == "__main__":
    main()
