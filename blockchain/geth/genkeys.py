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
    #este passo compreende a geração de um par de chaves ECDSA secp256k1 —
    acc = Account.create()
    return acc.address.lower(), acc.key.hex()  # (0x..., 0x...)


def enode_pubkey(priv_hex: str) -> str:
    """Retorna 128 chars hex da chave pública P2P (formato enode://)."""
    #este passo compreende a derivação da chave pública P2P do nó — o geth usa a mesma
    #chave privada do sealer tanto para assinar blocos Clique quanto para o protocolo
    #de descoberta de peers (devp2p); o enode:// identifica o nó na rede pelo IP, porta
    #e pela chave pública (64 bytes = 128 hex chars) sem o prefixo 0x04 de ponto não comprimido
    priv_bytes = bytes.fromhex(priv_hex.replace("0x", ""))
    pub = eth_keys_lib.PrivateKey(priv_bytes).public_key
    return pub.to_hex()[2:]  # remove '0x' — 128 hex chars


def contract_address(deployer: str, nonce: int = 0) -> str:
    """Endereço determinístico do contrato (CREATE com nonce=0)."""
    #este passo compreende o pré-cálculo do endereço do contrato antes mesmo de ele
    #existir — pela fórmula CREATE do EVM: keccak256(RLP([deployer, nonce]))[-20 bytes];
    #com nonce=0 (primeira transação do deployer) o endereço é fixo e pode ser escrito
    #diretamente no .env e distribuído para todos os serviços sem aguardar o deploy
    addr_bytes = bytes.fromhex(deployer.lower().replace("0x", ""))
    encoded    = rlp.encode([addr_bytes, nonce])
    return "0x" + Web3.keccak(encoded).hex()[-40:]


def build_extradata(sealer_addrs: list[str]) -> str:
    """32 zeros + sealers ordenados (sem 0x) + 65 zeros."""
    #este passo compreende a montagem do campo extraData do genesis.json no formato
    #exigido pelo Clique PoA: 32 bytes de zeros (vanity), seguidos dos endereços dos
    #sealers autorizados concatenados em ordem lexicográfica crescente (20 bytes cada),
    #seguidos de 65 bytes de zeros reservados para a assinatura do primeiro bloco;
    #qualquer nó que não esteja nesta lista será rejeitado como sealer pela rede
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

    #este passo compreende a atualização do genesis.json com os novos sealers e saldos
    #iniciais — o extraData embute a lista de sealers autorizados para o Clique validar
    #quem pode selar blocos; o alloc pré-distribui 1000 ETH (em wei) para cada conta
    #gerada, garantindo gas suficiente para deploy, mint e logs sem precisar de faucet
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

    #este passo compreende a geração do arquivo .env com todas as chaves privadas e
    #endereços — o docker-compose lê este arquivo via ${VAR} e injeta as credenciais
    #corretas em cada container; o NODE_KEY (sem prefixo 0x) é passado ao geth via
    #--nodekeyhex para que cada nó use a chave privada do seu sealer como identidade P2P
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
        "# IPs das máquinas (execução distribuída)",
        "IP_SETOR_1=172.16.103.2",
        "IP_SETOR_2=172.16.103.3",
        "IP_SETOR_3=172.16.103.10",
        "IP_SETOR_4=172.16.103.11",
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
