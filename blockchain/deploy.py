import json
import os
import socket
import time

from solcx import compile_source, install_solc
from web3 import Web3

GANACHE_URL    = os.environ.get("GANACHE_URL",    "http://ganache:8545")
INITIAL_TOKENS = int(os.environ.get("INITIAL_TOKENS", "500"))

# Publica o contrato em todos os nós para eliminar SPOF de distribuição
ALL_NODES = [
    (os.environ.get("NODE_1_HOST", "sector_node_1"), int(os.environ.get("NODE_1_PORT", "3001"))),
    (os.environ.get("NODE_2_HOST", "sector_node_2"), int(os.environ.get("NODE_2_PORT", "3002"))),
    (os.environ.get("NODE_3_HOST", "sector_node_3"), int(os.environ.get("NODE_3_PORT", "3003"))),
    (os.environ.get("NODE_4_HOST", "sector_node_4"), int(os.environ.get("NODE_4_PORT", "3004"))),
]

# Contas determinísticas do Ganache com mnemônico padrão.
# A conta 0 é usada como deployer/owner; as demais são pré-financiadas com tokens.
GANACHE_ACCOUNTS = [
    "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",  # 0 — deployer
    "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",  # 1
    "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",  # 2
    "0x90F79bf6EB2c4f870365E785982E1f101E93b906",  # 3
    "0x15d34AAf54267DB7D7c367839AAf71A00a2C6A65",  # 4
    "0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc",  # 5
    "0x976EA74026E726554dB657fA54763abd0C3a0aa",   # 6
    "0x14dC79964da2C08b23698B3D3cc7Ca32193d9955",  # 7
    "0x23618e81E3f5cdF7f54C3d65f7FBc0aBf5B21E8",  # 8
    "0xa0Ee7A142d267C1f36714E4a8F75612F20a79720",  # 9
]

MQTT_TOPIC = "strait/blockchain/contract"


# ─── MQTT helper (sem dependência externa) ────────────────────────────────────

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


def mqtt_publish_retained(host, port, topic, payload):
    cid = b"blockchain_deployer"
    var = b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid) >> 8, len(cid) & 0xFF]) + cid
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((host, port))
    sock.sendall(bytes([0x10]) + _enc_rem(len(var)) + var)
    sock.recv(4)  # CONNACK

    if isinstance(payload, str):
        payload = payload.encode()
    tb  = topic.encode()
    var = bytes([len(tb) >> 8, len(tb) & 0xFF]) + tb + payload
    # retain flag = bit 0 do primeiro byte
    pkt = bytes([0x31]) + _enc_rem(len(var)) + var
    sock.sendall(pkt)
    time.sleep(0.5)
    sock.close()
    print(f"[DEPLOY] Contrato publicado em MQTT {topic} (retained)")


# ─── Aguardar serviços ────────────────────────────────────────────────────────

def wait_for_ganache():
    print(f"[DEPLOY] Aguardando Ganache em {GANACHE_URL}...")
    while True:
        try:
            w3 = Web3(Web3.HTTPProvider(GANACHE_URL))
            if w3.is_connected():
                print("[DEPLOY] Ganache pronto.")
                return w3
        except Exception:
            pass
        time.sleep(2)


def wait_for_broker(host, port):
    print(f"[DEPLOY] Aguardando nó {host}:{port}...")
    while True:
        try:
            s = socket.create_connection((host, port), timeout=3)
            s.close()
            print(f"[DEPLOY] Nó {host}:{port} pronto.")
            return
        except Exception:
            time.sleep(2)


# ─── Compilar e implantar ─────────────────────────────────────────────────────

def compile_contract():
    print("[DEPLOY] Instalando solc 0.8.20...")
    install_solc("0.8.20")
    with open("/app/DroneToken.sol") as f:
        source = f.read()
    compiled = compile_source(source, output_values=["abi", "bin"],
                              solc_version="0.8.20")
    _, contract_data = compiled.popitem()
    return contract_data["abi"], contract_data["bin"]


def deploy_contract(w3, abi, bytecode):
    deployer = w3.eth.accounts[0]
    Contract = w3.eth.contract(abi=abi, bytecode=bytecode)
    tx_hash  = Contract.constructor().transact({"from": deployer})
    receipt  = w3.eth.wait_for_transaction_receipt(tx_hash)
    address  = receipt.contractAddress
    print(f"[DEPLOY] Contrato implantado em {address}")
    return w3.eth.contract(address=address, abi=abi)


def mint_tokens(w3, contract):
    deployer = w3.eth.accounts[0]
    for addr in GANACHE_ACCOUNTS[1:]:  # pula o deployer
        tx = contract.functions.mint(addr, INITIAL_TOKENS).transact({"from": deployer})
        w3.eth.wait_for_transaction_receipt(tx)
        print(f"[DEPLOY] {INITIAL_TOKENS} tokens → {addr}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    w3 = wait_for_ganache()
    for host, port in ALL_NODES:
        wait_for_broker(host, port)

    abi, bytecode = compile_contract()
    contract      = deploy_contract(w3, abi, bytecode)
    mint_tokens(w3, contract)

    info = json.dumps({"address": contract.address, "abi": abi})
    for host, port in ALL_NODES:
        try:
            mqtt_publish_retained(host, port, MQTT_TOPIC, info)
        except Exception as e:
            print(f"[DEPLOY] Aviso: falha ao publicar em {host}:{port} — {e}")
    print("[DEPLOY] Concluído.")


if __name__ == "__main__":
    main()
