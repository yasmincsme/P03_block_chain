"""
Gera todas as contas Ethereum para a rede Clique privada e produz:
  - blockchain/geth/genesis.json  (sealers no extradata + alloc atualizado)
  - .env  (na raiz do projeto — lido pelo docker-compose para ${VAR})

Uso:
  pip install web3 eth-account rlp
  python3 blockchain/geth/genkeys.py   # execute da raiz do projeto

Contas geradas:
  sealer_1..4  → nós geth (mineradores Clique) + carteiras dos sector managers
  deployer     → implanta DroneToken.sol (owner do contrato)
  empresa_a    → cliente empresa A (500 tokens após deploy)
  empresa_b    → cliente empresa B (500 tokens após deploy)
"""

import json
import os
import sys

try:
    from eth_account import Account
    from eth_keys import keys as eth_keys_lib
    from web3 import Web3
    import rlp
except ImportError:
    print("Instale: pip install web3 eth-account eth-keys rlp")
    sys.exit(1)

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
GENESIS_PATH = os.path.join(SCRIPT_DIR, "genesis.json")
DOT_ENV_PATH = os.path.join(PROJECT_ROOT, ".env")


def gen_account() -> tuple[str, str]:
    acc = Account.create()
    return acc.address.lower(), acc.key.hex()  # (0x..., 0x...)


def enode_pubkey(priv_hex: str) -> str:
    """Retorna 128 chars hex da chave pública P2P (formato enode://)."""
    priv_bytes = bytes.fromhex(priv_hex.replace("0x", ""))
    pub = eth_keys_lib.PrivateKey(priv_bytes).public_key
    return pub.to_hex()[2:]  # remove '0x' — 128 hex chars


def contract_address(deployer: str, nonce: int = 0) -> str:
    """Endereço determinístico do contrato (CREATE com nonce=0)."""
    addr_bytes = bytes.fromhex(deployer.lower().replace("0x", ""))
    encoded    = rlp.encode([addr_bytes, nonce])
    return "0x" + Web3.keccak(encoded).hex()[-40:]


def build_extradata(sealer_addrs: list[str]) -> str:
    """32 zeros + sealers ordenados (sem 0x) + 65 zeros."""
    sealers = sorted(a.lower().replace("0x", "") for a in sealer_addrs)
    return "0x" + "00" * 32 + "".join(sealers) + "00" * 65


def main():
    print("Gerando contas para a rede Clique privada...\n")

    roles = ["sealer_1", "sealer_2", "sealer_3", "sealer_4",
             "deployer", "empresa_a", "empresa_b"]

    accounts: dict[str, tuple[str, str]] = {}
    for role in roles:
        addr, key = gen_account()
        accounts[role] = (addr, key)
        print(f"  {role:<12} {addr}  {key[:18]}...")

    sealers      = [accounts[f"sealer_{i}"][0] for i in range(1, 5)]
    deployer_addr = accounts["deployer"][0]
    contract_addr = contract_address(deployer_addr, 0)
    print(f"\n  contrato (deterministico) = {contract_addr}")

    # ── Atualiza genesis.json ─────────────────────────────────────────────────
    with open(GENESIS_PATH) as f:
        genesis = json.load(f)

    genesis["extradata"] = build_extradata(sealers)
    genesis["alloc"] = {
        addr: {"balance": "1000000000000000000000"}
        for _, (addr, _) in accounts.items()
    }
    with open(GENESIS_PATH, "w") as f:
        json.dump(genesis, f, indent=2)
    print(f"\n  genesis.json atualizado.")

    # ── Deriva enodes (pubkeys P2P) ───────────────────────────────────────────
    enodes = {}
    for i in range(1, 5):
        _, key = accounts[f"sealer_{i}"]
        enodes[i] = enode_pubkey(key)

    # ── Grava .env na raiz do projeto ─────────────────────────────────────────
    lines = [
        "# Gerado por blockchain/geth/genkeys.py",
        "# NUNCA commitar este arquivo em repositórios públicos",
        "",
    ]

    for i in range(1, 5):
        addr, key = accounts[f"sealer_{i}"]
        node_key  = key.replace("0x", "")  # --nodekeyhex não aceita 0x
        lines += [
            f"SEALER_{i}_ADDR={addr}",
            f"SEALER_{i}_KEY={key}",
            f"SEALER_{i}_NODE_KEY={node_key}",
            f"ENODE_{i}={enodes[i]}",
            "",
        ]

    deployer_key = accounts["deployer"][1]
    lines += [
        f"DEPLOYER_ADDR={deployer_addr}",
        f"DEPLOYER_KEY={deployer_key}",
        "",
        f"EMPRESA_A_ADDR={accounts['empresa_a'][0]}",
        f"EMPRESA_A_KEY={accounts['empresa_a'][1]}",
        "",
        f"EMPRESA_B_ADDR={accounts['empresa_b'][0]}",
        f"EMPRESA_B_KEY={accounts['empresa_b'][1]}",
        "",
        f"CONTRACT_ADDR={contract_addr}",
        "",
    ]

    with open(DOT_ENV_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"  .env gravado em {DOT_ENV_PATH}")
    print("\nPróximos passos:")
    print("  docker compose build")
    print("  docker compose up geth_setor_1 geth_setor_2 geth_setor_3 geth_setor_4")
    print("  docker compose up blockchain_deployer")
    print("  docker compose up  # restante dos serviços")


if __name__ == "__main__":
    main()
