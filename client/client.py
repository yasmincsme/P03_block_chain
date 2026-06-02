import json
import os
import socket
import sys
import threading
import time

from web3 import Web3

GANACHE_URL = os.environ.get("GANACHE_URL", "http://ganache:8545")

BROKERS = {
    1: (os.environ.get("BROKER_1_HOST", "localhost"), int(os.environ.get("BROKER_1_PORT", "3001"))),
    2: (os.environ.get("BROKER_2_HOST", "localhost"), int(os.environ.get("BROKER_2_PORT", "3002"))),
    3: (os.environ.get("BROKER_3_HOST", "localhost"), int(os.environ.get("BROKER_3_PORT", "3003"))),
    4: (os.environ.get("BROKER_4_HOST", "localhost"), int(os.environ.get("BROKER_4_PORT", "3004"))),
}

# Carteira configurável via variável de ambiente.
# Padrão: conta 1 do Ganache (mnemônico padrão de testes).
WALLET_ADDRESS     = os.environ.get("WALLET_ADDRESS",
                                    "0x70997970C51812dc3A010C7d01b50e0d17dc79C8")
WALLET_PRIVATE_KEY = os.environ.get("WALLET_PRIVATE_KEY",
                                    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d")

OCCURRENCE_TYPES = [
    ("bloqueio_de_rota",        4),
    ("embarcacao_a_deriva",     4),
    ("risco_ambiental",         4),
    ("falha_de_sinalizacao",    3),
    ("congestionamento",        3),
    ("inspecao_urgente",        3),
    ("objeto_nao_identificado", 2),
    ("inspecao_rotineira",      1),
]

COSTS = {4: 40, 3: 30, 2: 20, 1: 10}

_req_counter   = 0
_contract_info = {"address": None, "abi": None}
_contract_lock = threading.Event()


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


def _listen_contract_info():
    """Assina strait/blockchain/contract em broker_1 e aguarda o endereço do contrato."""
    host, port = BROKERS[1]
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((host, port))
            sock.settimeout(None)

            cid = b"drone_client_contract_sub"
            var = b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid
            sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
            _read_exact(sock, 4)  # CONNACK

            topic = b"strait/blockchain/contract"
            var   = bytes([0, 1, len(topic) >> 8, len(topic) & 0xFF]) + topic + b"\x00"
            sock.sendall(bytes([0x82]) + _enc_rem(len(var)) + var)
            _read_exact(sock, 5)  # SUBACK

            while True:
                hdr = sock.recv(1)
                if not hdr:
                    break
                ptype = (hdr[0] >> 4) & 0x0F
                rem   = _dec_rem(sock)
                if rem is None:
                    break
                data = _read_exact(sock, rem) if rem else b""
                if ptype == 3:
                    tlen    = (data[0] << 8) | data[1]
                    payload = data[2 + tlen:]
                    info    = json.loads(payload)
                    _contract_info["address"] = info["address"]
                    _contract_info["abi"]     = info["abi"]
                    _contract_lock.set()
                elif ptype == 12:
                    sock.sendall(bytes([0xD0, 0x00]))
        except Exception:
            pass
        time.sleep(5)


# ─── Web3 / contrato ──────────────────────────────────────────────────────────

def get_balance(w3, contract):
    return contract.functions.balances(
        Web3.to_checksum_address(WALLET_ADDRESS)
    ).call()


def send_request(w3, contract, sector, occ_type, criticality, req_id):
    account = w3.eth.account.from_key(WALLET_PRIVATE_KEY)
    tx      = contract.functions.requestDrone(
        sector, occ_type, criticality, req_id
    ).build_transaction({
        "from":     account.address,
        "nonce":    w3.eth.get_transaction_count(account.address),
        "gas":      200_000,
        "gasPrice": w3.eth.gas_price,
    })
    signed  = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    return tx_hash.hex(), receipt.status  # status 1 = sucesso


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    threading.Thread(target=_listen_contract_info, daemon=True).start()

    print("=" * 56)
    print("   CLIENTE DE SOLICITAÇÃO DE DRONE — BLOCKCHAIN")
    print("=" * 56)
    print(f"\n  Carteira : {WALLET_ADDRESS}")
    print(f"  Ganache  : {GANACHE_URL}")
    print("\n  Aguardando endereço do contrato via MQTT...", end=" ", flush=True)

    _contract_lock.wait()
    print("OK")

    w3       = Web3(Web3.HTTPProvider(GANACHE_URL))
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(_contract_info["address"]),
        abi=_contract_info["abi"],
    )

    occ_menu = "\nTipos de ocorrência:\n"
    for i, (name, crit) in enumerate(OCCURRENCE_TYPES, 1):
        cost = COSTS.get(crit, 10)
        occ_menu += f"  {i}. {name:<30} crit={crit}  custo={cost} tokens\n"

    while True:
        saldo = get_balance(w3, contract)
        print(f"\n{'─' * 56}")
        print(f"  Saldo: {saldo} tokens")

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
        cost = COSTS.get(criticality, 10)

        if saldo < cost:
            print(f"  Saldo insuficiente: {saldo} tokens (necessário: {cost})")
            continue

        global _req_counter
        _req_counter += 1
        req_id = f"req_{sector}_{_req_counter:04d}"

        print(f"\n  Enviando transação blockchain...", end=" ", flush=True)
        try:
            tx_hash, status = send_request(w3, contract, sector, occ_type, criticality, req_id)
            if status == 1:
                print(f"OK\n  tx={tx_hash[:18]}...\n  Setor {sector} receberá a ocorrência.")
            else:
                print(f"REVERTIDA\n  tx={tx_hash[:18]}...")
        except Exception as e:
            print(f"ERRO: {e}")


if __name__ == "__main__":
    main()
