"""
Auditoria da blockchain DroneToken — consulta imutável, sem permissões especiais.

Uso:
  python3 blockchain/audit.py [--url http://localhost:18541] [--from-block 0]

Exibe:
  - Saldo de tokens de todas as contas conhecidas
  - Histórico de eventos: DroneRequested, DroneDispatched, OccurrenceRequeued,
    DroneFailed, DroneRecalled, Transfer
"""

import argparse
import json
import os
import sys

try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
except ImportError:
    print("Instale: pip install web3")
    sys.exit(1)

DEFAULT_URL      = os.environ.get("GETH_URL",      "http://localhost:18541")
CONTRACT_ADDR    = os.environ.get("CONTRACT_ADDR",  "")

#este passo compreende a definição da ABI completa de auditoria — ao contrário das ABIs
#parciais do client e do sector_manager, aqui são necessárias todas as assinaturas de
#eventos para decodificar os logs armazenados nos blocos; sem a assinatura correta o
#web3 não consegue calcular o topic0 (keccak do nome+tipos) nem decodificar os argumentos
FULL_ABI = [
    {"name": "balances", "type": "function", "stateMutability": "view",
     "outputs": [{"type": "uint256"}],
     "inputs":  [{"name": "account", "type": "address"}]},
    {"name": "owner",    "type": "function", "stateMutability": "view",
     "outputs": [{"type": "address"}], "inputs": []},

    {"name": "DroneRequested",     "type": "event", "anonymous": False,
     "inputs": [
         {"name": "requester",      "type": "address", "indexed": True},
         {"name": "sector",         "type": "uint8",   "indexed": True},
         {"name": "occurrenceType", "type": "string",  "indexed": False},
         {"name": "criticality",    "type": "uint8",   "indexed": False},
         {"name": "cost",           "type": "uint256", "indexed": False},
         {"name": "requestId",      "type": "string",  "indexed": False},
         {"name": "ts",             "type": "uint256", "indexed": False},
     ]},
    {"name": "DroneDispatched",    "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",         "type": "uint8",  "indexed": True},
         {"name": "occurrenceId",   "type": "string", "indexed": False},
         {"name": "droneId",        "type": "string", "indexed": False},
         {"name": "requestId",      "type": "string", "indexed": False},
         {"name": "ts",             "type": "uint256","indexed": False},
     ]},
    {"name": "OccurrenceRequeued", "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",         "type": "uint8",  "indexed": True},
         {"name": "occurrenceId",   "type": "string", "indexed": False},
         {"name": "reason",         "type": "string", "indexed": False},
         {"name": "ts",             "type": "uint256","indexed": False},
     ]},
    {"name": "DroneFailed",        "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",         "type": "uint8",  "indexed": True},
         {"name": "occurrenceId",   "type": "string", "indexed": False},
         {"name": "droneId",        "type": "string", "indexed": False},
         {"name": "ts",             "type": "uint256","indexed": False},
     ]},
    {"name": "DroneRecalled",      "type": "event", "anonymous": False,
     "inputs": [
         {"name": "sector",         "type": "uint8",  "indexed": True},
         {"name": "droneId",        "type": "string", "indexed": False},
         {"name": "occurrenceId",   "type": "string", "indexed": False},
         {"name": "ts",             "type": "uint256","indexed": False},
     ]},
    {"name": "Transfer",           "type": "event", "anonymous": False,
     "inputs": [
         {"name": "from",   "type": "address", "indexed": True},
         {"name": "to",     "type": "address", "indexed": True},
         {"name": "amount", "type": "uint256", "indexed": False},
     ]},
]

EVENT_NAMES = [
    "DroneRequested", "DroneDispatched", "OccurrenceRequeued",
    "DroneFailed", "DroneRecalled", "Transfer",
]


def fmt_ts(ts: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def print_balances(contract, w3: Web3) -> None:
    print("\n" + "═" * 60)
    print("  SALDOS DE TOKENS")
    print("═" * 60)

    #este passo compreende a descoberta das contas conhecidas via .env — a blockchain
    #armazena apenas endereços (hashes), sem nomes; ler o .env permite exibir rótulos
    #legíveis (empresa_a, sealer_1 etc.) em vez de endereços brutos; balances() é uma
    #chamada view sem gas, executada localmente no nó via eth_call
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    addrs: dict[str, str] = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "_ADDR=" in line and not line.startswith("#") and not line.startswith("CONTRACT_ADDR"):
                    key, val = line.split("=", 1)
                    label = key.replace("_ADDR", "").replace("_", " ").lower()
                    addrs[label] = val.strip()

    if not addrs:
        print("  (sem .env — forneça endereços manualmente)")
        return

    for label, addr in addrs.items():
        try:
            bal = contract.functions.balances(Web3.to_checksum_address(addr)).call()
            print(f"  {label:<20} {addr}  →  {bal:>6} tokens")
        except Exception as e:
            print(f"  {label:<20} {addr}  →  erro: {e}")


def print_events(contract, w3: Web3, from_block: int) -> None:
    print("\n" + "═" * 60)
    print(f"  HISTÓRICO DE EVENTOS (a partir do bloco {from_block})")
    print("═" * 60)

    #este passo compreende a coleta de eventos via eth_getLogs — para cada tipo de evento
    #calcula o topic0 (keccak256 da assinatura do evento) e consulta todos os logs do
    #contrato que correspondem a esse topic; os logs ficam gravados permanentemente nos
    #blocos e podem ser consultados por qualquer nó sem permissão especial; ao final ordena
    #por (blockNumber, transactionIndex) para garantir ordem cronológica exata de execução
    all_events = []
    for name in EVENT_NAMES:
        try:
            logs = w3.eth.get_logs({
                "fromBlock": from_block,
                "toBlock":   "latest",
                "address":   contract.address,
                "topics":    [getattr(contract.events, name).build_filter().topics[0]],
            })
            ev_proc = getattr(contract.events, name)
            for raw in logs:
                ev = ev_proc().process_log(raw)
                all_events.append((ev["blockNumber"], ev["transactionIndex"], name, ev["args"]))
        except Exception as exc:
            print(f"  [aviso] {name}: {exc}")

    all_events.sort(key=lambda x: (x[0], x[1]))

    if not all_events:
        print("  Nenhum evento encontrado.")
        return

    for block, _tx_idx, name, args in all_events:
        ts  = args.get("ts", 0)
        ts_str = fmt_ts(ts) if ts else f"bloco {block}"

        if name == "DroneRequested":
            line = (f"  [{ts_str}] SOLICITADO  setor={args['sector']}"
                    f"  tipo={args['occurrenceType']}"
                    f"  crit={args['criticality']}  custo={args['cost']}"
                    f"  req={str(args['requestId'])[:16]}..."
                    f"  de={str(args['requester'])[:12]}...")
        elif name == "DroneDispatched":
            line = (f"  [{ts_str}] DESPACHADO  setor={args['sector']}"
                    f"  occ={args['occurrenceId']}"
                    f"  drone={args['droneId']}")
        elif name == "OccurrenceRequeued":
            line = (f"  [{ts_str}] REENFILEIRADO setor={args['sector']}"
                    f"  occ={args['occurrenceId']}"
                    f"  motivo={args['reason'][:40]}")
        elif name == "DroneFailed":
            line = (f"  [{ts_str}] FALHA       setor={args['sector']}"
                    f"  occ={args['occurrenceId']}"
                    f"  drone={args['droneId']}")
        elif name == "DroneRecalled":
            line = (f"  [{ts_str}] RETORNO     setor={args['sector']}"
                    f"  drone={args['droneId']}"
                    f"  occ={args['occurrenceId']}")
        elif name == "Transfer":
            frm = str(args["from"])
            to  = str(args["to"])
            if frm == "0x0000000000000000000000000000000000000000":
                line = f"  [{ts_str}] MINT        {to[:14]}...  +{args['amount']} tokens"
            else:
                line = (f"  [{ts_str}] TRANSFER    {frm[:12]}..."
                        f"  →  {to[:12]}...  {args['amount']} tokens")
        else:
            line = f"  [{ts_str}] {name}  {dict(args)}"

        print(line)


def main() -> None:
    parser = argparse.ArgumentParser(description="Audita a blockchain DroneToken")
    parser.add_argument("--url",        default=DEFAULT_URL,   help="URL do nó geth (HTTP RPC)")
    parser.add_argument("--contract",   default=CONTRACT_ADDR, help="Endereço do contrato")
    parser.add_argument("--from-block", default=0, type=int,   help="Bloco inicial (default: 0)")
    args = parser.parse_args()

    if not args.contract:
        print("Erro: forneça --contract 0x... ou defina CONTRACT_ADDR no ambiente.")
        sys.exit(1)

    print(f"Conectando a {args.url}...")
    #este passo compreende a conexão ao nó e a injeção do middleware PoA — qualquer nó
    #geth da rede serve para auditoria (acesso é somente leitura); o geth_poa_middleware
    #é obrigatório pelo mesmo motivo dos outros serviços: o Clique adiciona 65 bytes
    #extras no extraData do cabeçalho que o web3 vanilla trata como bloco malformado
    w3 = Web3(Web3.HTTPProvider(args.url, request_kwargs={"timeout": 15}))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    if not w3.is_connected():
        print("Falha na conexão.")
        sys.exit(1)

    print(f"Conectado — bloco #{w3.eth.block_number}  chainId={w3.eth.chain_id}")

    contract = w3.eth.contract(
        address=Web3.to_checksum_address(args.contract),
        abi=FULL_ABI,
    )

    print_balances(contract, w3)
    print_events(contract, w3, args.from_block)

    print("\n" + "═" * 60)


if __name__ == "__main__":
    main()
