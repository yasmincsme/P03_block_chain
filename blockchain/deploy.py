"""
Implanta DroneToken.sol na rede geth Clique (PoA privada) e cunha tokens
para as contas empresa_a e empresa_b.

O endereço do contrato é determinístico (deployer nonce=0), portanto não
precisa ser distribuído via MQTT — todos os serviços o calculam localmente.

Contas (geradas pelo genkeys.sh — ver blockchain/geth/):
  deployer   → DEPLOY_ADDR / DEPLOY_KEY   (conta 0, dono do contrato)
  empresa_a  → COMPANY_A_ADDR / ...       (500 tokens iniciais)
  empresa_b  → COMPANY_B_ADDR / ...       (500 tokens iniciais)
  setor_1..4 → SETOR_N_ADDR / ...         (apenas ETH para gas)
"""

import json
import os
import time

from web3 import Web3
from web3.middleware import geth_poa_middleware
from solcx import compile_source, install_solc

# ─── Variáveis de ambiente ────────────────────────────────────────────────────

GETH_URL      = os.environ.get("GETH_URL",      "http://geth_setor_1:8545")
DEPLOY_ADDR   = os.environ.get("DEPLOY_ADDR",   "")
DEPLOY_KEY    = os.environ.get("DEPLOY_KEY",    "")
COMPANY_A_ADDR = os.environ.get("COMPANY_A_ADDR", "")
COMPANY_B_ADDR = os.environ.get("COMPANY_B_ADDR", "")
TOKENS_INITIAL = int(os.environ.get("TOKENS_INITIAL", "500"))

SOL_PATH = os.path.join(os.path.dirname(__file__), "DroneToken.sol")


# ─── Aguardar geth estar minerando ───────────────────────────────────────────

def wait_for_geth(w3: Web3, timeout: int = 180) -> None:
    print(f"Aguardando geth em {GETH_URL}...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if w3.is_connected():
                print(f"  conectado — bloco #{w3.eth.block_number}", flush=True)
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"geth não respondeu em {timeout}s")


# ─── Compilar contrato ────────────────────────────────────────────────────────

def compile_contract() -> tuple[list, str]:
    print("Instalando solc 0.8.20...", end=" ", flush=True)
    install_solc("0.8.20")
    print("OK")

    with open(SOL_PATH) as f:
        source = f.read()

    print("Compilando DroneToken.sol...", end=" ", flush=True)
    compiled = compile_source(source, output_values=["abi", "bin"],
                               solc_version="0.8.20")
    _, iface = next(iter(compiled.items()))
    print("OK")
    return iface["abi"], iface["bin"]


# ─── Deploy ───────────────────────────────────────────────────────────────────

def _deterministic_address(deployer_addr: str) -> str:
    """Endereço que o contrato terá quando deployer enviar sua primeira tx (nonce=0)."""
    import rlp as _rlp
    raw = bytes.fromhex(deployer_addr.lower().replace("0x", ""))
    return "0x" + Web3.keccak(_rlp.encode([raw, 0])).hex()[-40:]


def deploy(w3: Web3, abi: list, bytecode: str) -> object:
    deployer  = Web3.to_checksum_address(DEPLOY_ADDR)
    key       = DEPLOY_KEY if DEPLOY_KEY.startswith("0x") else "0x" + DEPLOY_KEY

    # Idempotente: se o contrato já existe no endereço determinístico, reutiliza
    expected = Web3.to_checksum_address(_deterministic_address(DEPLOY_ADDR))
    code = w3.eth.get_code(expected)  # falha explicitamente se geth não responde
    if len(code) > 0:
        print(f"  Contrato já implantado em {expected} — reutilizando.")
        return w3.eth.contract(address=expected, abi=abi)

    gas_price = w3.eth.gas_price
    nonce     = w3.eth.get_transaction_count(deployer, "pending")
    Contract  = w3.eth.contract(abi=abi, bytecode=bytecode)

    tx = Contract.constructor().build_transaction({
        "from":     deployer,
        "nonce":    nonce,
        "gas":      3_000_000,
        "gasPrice": gas_price,
    })
    signed = w3.eth.account.sign_transaction(tx, key)
    try:
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    except ValueError as e:
        err = str(e).lower()
        if "already known" in err or "nonce too low" in err:
            tx_hash = signed.hash
            print("  Transação já no mempool, aguardando confirmação...", end=" ", flush=True)
        else:
            raise

    print(f"  Implantando contrato (tx {tx_hash.hex()[:20]}...)...", end=" ", flush=True)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
    addr    = receipt.contractAddress
    print(f"OK → {addr}")
    return w3.eth.contract(address=addr, abi=abi)


# ─── Mint ─────────────────────────────────────────────────────────────────────

def mint_tokens(w3: Web3, contract, recipient: str, amount: int) -> None:
    deployer  = Web3.to_checksum_address(DEPLOY_ADDR)
    key       = DEPLOY_KEY if DEPLOY_KEY.startswith("0x") else "0x" + DEPLOY_KEY
    recipient = Web3.to_checksum_address(recipient)

    current = contract.functions.balances(recipient).call()
    if current >= amount:
        print(f"  {recipient} já tem {current} tokens — skipping.")
        return

    nonce     = w3.eth.get_transaction_count(deployer, "pending")
    gas_price = w3.eth.gas_price

    tx = contract.functions.mint(recipient, amount).build_transaction({
        "from":     deployer,
        "nonce":    nonce,
        "gas":      100_000,
        "gasPrice": gas_price,
    })
    signed = w3.eth.account.sign_transaction(tx, key)
    try:
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    except ValueError as e:
        err = str(e).lower()
        if "already known" in err or "nonce too low" in err:
            tx_hash = signed.hash
        else:
            raise

    print(f"  {recipient} → {amount} tokens  (tx {tx_hash.hex()[:16]}...)...", end=" ", flush=True)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=300)
    print("OK")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 56)
    print("   DEPLOY — DroneToken (rede geth PoA)")
    print("=" * 56)

    if not DEPLOY_ADDR or not DEPLOY_KEY:
        raise RuntimeError("DEPLOY_ADDR e DEPLOY_KEY são obrigatórios")

    w3 = Web3(Web3.HTTPProvider(GETH_URL))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    wait_for_geth(w3)

    abi, bytecode = compile_contract()
    contract      = deploy(w3, abi, bytecode)

    print(f"\nCunhando tokens ({TOKENS_INITIAL} por empresa)...")
    for addr in [COMPANY_A_ADDR, COMPANY_B_ADDR]:
        if addr:
            mint_tokens(w3, contract, addr, TOKENS_INITIAL)

    # Salva ABI localmente para referência dos outros serviços
    out = {"address": contract.address, "abi": abi}
    out_path = os.path.join(os.path.dirname(__file__), "contract_info.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nABI salvo em {out_path}")

    print("\nDeploy concluído.")
    print(f"  Endereço : {contract.address}")
    print(f"  Chain ID : {w3.eth.chain_id}")
    print(f"  Bloco    : #{w3.eth.block_number}")


if __name__ == "__main__":
    main()
