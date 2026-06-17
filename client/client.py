"""
Cliente empresa — solicita drones pagando tokens no DroneToken.sol.

Fluxo por solicitação:
  1. Mostra saldo de tokens
  2. Usuário escolhe setor e tipo de ocorrência
  3. Chama requestDrone() no contrato (debita tokens atomicamente)
  4. Publica manual_request no MQTT (com request_id = tx hash)
  5. O sector_manager valida e despacha o drone

Também permite transferir tokens entre empresas.
"""

import json
import os
import socket
import sys
import threading
import time
import uuid

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "3001"))
CLIENT_ID   = os.environ.get("CLIENT_ID",   "empresa_a")

GETH_URL      = os.environ.get("GETH_URL",      "http://geth_setor_1:8545")
WALLET_ADDR   = os.environ.get("WALLET_ADDR",   "")
WALLET_KEY    = os.environ.get("WALLET_KEY",    "")
CONTRACT_ADDR = os.environ.get("CONTRACT_ADDR", "")

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

TOKEN_COST = {4: 40, 3: 30, 2: 20, 1: 10}

_CLIENT_ABI = [
    {"name": "balances",     "type": "function", "stateMutability": "view",
     "outputs": [{"type": "uint256"}],
     "inputs": [{"name": "account", "type": "address"}]},
    {"name": "requestDrone", "type": "function", "stateMutability": "nonpayable",
     "outputs": [],
     "inputs": [{"name": "sector",         "type": "uint8"},
                {"name": "occurrenceType", "type": "string"},
                {"name": "criticality",    "type": "uint8"},
                {"name": "requestId",      "type": "string"}]},
    {"name": "transfer",     "type": "function", "stateMutability": "nonpayable",
     "outputs": [],
     "inputs": [{"name": "to",     "type": "address"},
                {"name": "amount", "type": "uint256"}]},
]


# ─── MQTT helpers (raw socket) ────────────────────────────────────────────────

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
                print(f"  [MQTT] Publish falhou: {e}")

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
                    qos  = (flags >> 1) & 0x03
                    tlen = (data[0] << 8) | data[1]
                    off  = 2 + tlen
                    if qos > 0:
                        mid = (data[off] << 8) | data[off + 1]
                        off += 2
                        with self._lock:
                            self._sock.sendall(bytes([0x40, 0x02, mid >> 8, mid & 0xFF]))
                elif ptype == 12:
                    with self._lock:
                        self._sock.sendall(bytes([0xD0, 0x00]))
            except Exception:
                break


# ─── Blockchain ───────────────────────────────────────────────────────────────

class BlockchainClient:

    def __init__(self):
        self._w3       = None
        self._contract = None
        self._nonce    = None
        self._addr     = None
        self._key      = None
        self.enabled   = False

        if not (WALLET_ADDR and WALLET_KEY and CONTRACT_ADDR and GETH_URL):
            print("\n  [blockchain] Sem configuração — modo MQTT puro (sem cobrança de tokens).")
            return

        try:
            from web3 import Web3
            from web3.middleware import geth_poa_middleware

            w3 = Web3(Web3.HTTPProvider(GETH_URL, request_kwargs={"timeout": 10}))
            w3.middleware_onion.inject(geth_poa_middleware, layer=0)

            self._w3       = w3
            self._addr     = Web3.to_checksum_address(WALLET_ADDR)
            self._key      = WALLET_KEY if WALLET_KEY.startswith("0x") else "0x" + WALLET_KEY
            self._contract = w3.eth.contract(
                address=Web3.to_checksum_address(CONTRACT_ADDR),
                abi=_CLIENT_ABI,
            )
            self.enabled = True
        except ImportError:
            print("\n  [blockchain] web3 não instalado — modo MQTT puro.")
        except Exception as exc:
            print(f"\n  [blockchain] Falha ao conectar: {exc}")

    def wait_for_geth(self, timeout: int = 120) -> bool:
        if not self.enabled:
            return False
        print("  Aguardando geth...", end=" ", flush=True)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self._w3.is_connected() and self._w3.eth.block_number >= 1:
                    print(f"OK (bloco #{self._w3.eth.block_number})")
                    return True
            except Exception:
                pass
            time.sleep(3)
        print("TIMEOUT")
        return False

    def balance(self) -> int:
        if not self.enabled:
            return -1
        try:
            return self._contract.functions.balances(self._addr).call()
        except Exception:
            return -1

    def request_drone(self, sector: int, occ_type: str, criticality: int) -> str | None:
        """Debita tokens e retorna o request_id (tx hash) ou None em caso de falha."""
        if not self.enabled:
            return f"local_{uuid.uuid4().hex[:12]}"

        if self._nonce is None:
            self._nonce = self._w3.eth.get_transaction_count(self._addr)

        request_id = f"req_{uuid.uuid4().hex[:12]}"
        try:
            tx = self._contract.functions.requestDrone(
                sector, occ_type, criticality, request_id
            ).build_transaction({
                "from":     self._addr,
                "nonce":    self._nonce,
                "gas":      150_000,
                "gasPrice": self._w3.eth.gas_price,
            })
            signed  = self._w3.eth.account.sign_transaction(tx, self._key)
            tx_hash = self._w3.eth.send_raw_transaction(signed.rawTransaction)
            self._nonce += 1
            print(f"  [blockchain] tx: {tx_hash.hex()[:22]}...", flush=True)
            print("  Aguardando confirmação...", end=" ", flush=True)
            self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            print("OK")
            return tx_hash.hex()
        except Exception as exc:
            print(f"\n  [blockchain] ERRO: {exc}")
            self._nonce = None
            return None

    def transfer(self, to_addr: str, amount: int) -> bool:
        if not self.enabled:
            print("  [blockchain] Transferência indisponível sem conexão.")
            return False

        if self._nonce is None:
            self._nonce = self._w3.eth.get_transaction_count(self._addr)

        try:
            to = self._w3.to_checksum_address(to_addr)
            tx = self._contract.functions.transfer(to, amount).build_transaction({
                "from":     self._addr,
                "nonce":    self._nonce,
                "gas":      80_000,
                "gasPrice": self._w3.eth.gas_price,
            })
            signed  = self._w3.eth.account.sign_transaction(tx, self._key)
            tx_hash = self._w3.eth.send_raw_transaction(signed.rawTransaction)
            self._nonce += 1
            self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            print(f"  Transferência de {amount} tokens confirmada.")
            return True
        except Exception as exc:
            print(f"\n  [blockchain] ERRO na transferência: {exc}")
            self._nonce = None
            return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 56)
    print("   CLIENTE EMPRESA — SOLICITAÇÃO DE DRONES")
    print("=" * 56)
    print(f"\n  Empresa  : {CLIENT_ID}")
    print(f"  Broker   : {BROKER_HOST}:{BROKER_PORT}")
    print(f"  Carteira : {WALLET_ADDR or '(nao configurada)'}")
    print(f"  Contrato : {CONTRACT_ADDR or '(nao configurado)'}")

    bc = BlockchainClient()
    if bc.enabled:
        bc.wait_for_geth()

    print("\n  Conectando ao broker MQTT...", end=" ", flush=True)
    mqtt = MQTTClient(BROKER_HOST, BROKER_PORT, CLIENT_ID)
    try:
        mqtt.connect()
    except ConnectionError as e:
        print(f"FALHA\n  {e}")
        sys.exit(1)
    print("OK\n")

    occ_menu = "Tipos de ocorrencia:\n"
    for i, (name, crit) in enumerate(OCCURRENCE_TYPES, 1):
        cost = TOKEN_COST.get(crit, 10)
        occ_menu += f"  {i}. {name:<32} crit={crit}  custo={cost} tokens\n"

    while True:
        print(f"{'─' * 56}")
        bal = bc.balance()
        if bal >= 0:
            print(f"  Saldo atual: {bal} tokens")

        print("  [1-4] solicitar drone  |  [t] transferir tokens  |  [q] sair")
        try:
            raw = input("\n  Setor destino [1-4 / t / q]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando.")
            sys.exit(0)

        if raw == "q":
            print("Encerrando.")
            sys.exit(0)

        if raw == "t":
            try:
                to_addr = input("  Endereço destino (0x...): ").strip()
                amount  = int(input("  Quantidade de tokens: ").strip())
            except (EOFError, KeyboardInterrupt, ValueError):
                print("  Cancelado.\n")
                continue
            bc.transfer(to_addr, amount)
            print()
            continue

        if raw not in ("1", "2", "3", "4"):
            print("  Entrada invalida.\n")
            continue

        sector = int(raw)
        print(f"\n{occ_menu}")

        try:
            raw2 = input(f"  Ocorrencia [1-{len(OCCURRENCE_TYPES)}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando.")
            sys.exit(0)

        if not raw2.isdigit() or not (1 <= int(raw2) <= len(OCCURRENCE_TYPES)):
            print("  Opcao invalida.\n")
            continue

        occ_type, criticality = OCCURRENCE_TYPES[int(raw2) - 1]
        cost = TOKEN_COST.get(criticality, 10)

        if bal >= 0 and bal < cost:
            print(f"\n  Saldo insuficiente ({bal} tokens, necessario {cost}).\n")
            continue

        print(f"\n  Solicitando '{occ_type}' no setor {sector} (custo={cost} tokens)...")
        request_id = bc.request_drone(sector, occ_type, criticality)

        if request_id is None:
            print("  Transacao falhou — solicitacao cancelada.\n")
            continue

        payload = json.dumps({
            "type":        occ_type,
            "criticality": criticality,
            "request_id":  request_id,
        })
        mqtt.publish(f"strait/sector/{sector}/manual_request", payload)
        print(f"  Enviado → setor {sector} | id={request_id[:24]}...\n")


if __name__ == "__main__":
    main()
