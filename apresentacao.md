---
marp: true
theme: default
paginate: true
style: |
  section {
    font-size: 22px;
    font-family: 'Segoe UI', sans-serif;
  }
  h1 { color: #1a1a2e; font-size: 2em; }
  h2 { color: #16213e; border-bottom: 3px solid #e94560; padding-bottom: 8px; }
  h3 { color: #0f3460; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; font-size: 0.85em; }
  pre { background: #1e1e1e; color: #d4d4d4; padding: 16px; border-radius: 8px; font-size: 0.75em; }
  .rubric { background: #e8f5e9; border-left: 4px solid #4caf50; padding: 8px 12px; margin: 8px 0; border-radius: 4px; font-size: 0.85em; }
  .theory { background: #e3f2fd; border-left: 4px solid #2196f3; padding: 8px 12px; margin: 8px 0; border-radius: 4px; }
  .warning { background: #fff3e0; border-left: 4px solid #ff9800; padding: 8px 12px; margin: 8px 0; border-radius: 4px; }
  table { font-size: 0.8em; width: 100%; }
  th { background: #16213e; color: white; }
---

<!-- _paginate: false -->

# Sistema de Monitoramento do Estreito Marítimo

## Blockchain + IoT Distribuído

**TEC 502 — Concorrência e Conectividade**
Universidade Estadual de Feira de Santana (UEFS)

Yasmin Cordeiro Meira

---

## Agenda

1. Visão Geral do Sistema
2. Teoria: O que é Blockchain?
3. Escolha Tecnológica — Por que Ethereum?
4. Rede Privada: Clique PoA (Proof of Authority)
5. Contrato Inteligente: DroneToken
6. Camada IoT: MQTT + Ricart-Agrawala
7. Fluxo Completo de uma Solicitação
8. Prevenção de Duplo Gasto
9. Log Imutável e Auditabilidade
10. Demo — Roteiro de Demonstração

---

## Visão Geral do Sistema

Monitoramento marítimo com **4 setores independentes**. Empresas solicitam drones pagando tokens. Tudo registrado em uma blockchain privada descentralizada.

```
EMPRESAS         BLOCKCHAIN (Ethereum)        IoT (MQTT + RA)
                                        
empresa_a  ─────► requestDrone()  ──────►  sector_manager_1 ──► drone_a
                  [debita tokens]           [Ricart-Agrawala]
                  [emite evento]            [dispatch MQTT]
                  confirmado                [recordDispatch()]
```

**Dois problemas distintos, duas soluções:**

| Problema | Solução |
|---|---|
| Gasto duplicado de tokens | Smart Contract (EVM serializa tudo) |
| Dois gerenciadores no mesmo drone | Ricart-Agrawala (exclusão mútua em ms) |

---

<!-- _class: teoria -->

## O que é Blockchain? — Conceito Fundamental

<div class="theory">

**Blockchain** é um registro distribuído (ledger) onde os dados são organizados em **blocos encadeados por hash criptográfico**. Uma vez escrito, um bloco não pode ser alterado sem invalidar todos os blocos seguintes.

</div>

```
  Bloco 0 (Gênese)      Bloco 1              Bloco 2
┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ parentHash: 0x0 │  │ parentHash: H(B0)│  │ parentHash: H(B1)│
│ txs: []         │◄─│ txs: [mint 500T] │◄─│ txs: [req drone] │
│ hash: H(B0)     │  │ hash: H(B1)      │  │ hash: H(B2)      │
└─────────────────┘  └──────────────────┘  └──────────────────┘
```

**Por que é imutável?**
Alterar qualquer byte no Bloco 1 muda `H(B1)` → invalida `parentHash` do Bloco 2 → invalida toda a cadeia subsequente. Os outros nós rejeitam a versão adulterada.

**Por que é descentralizado?**
Cada nó mantém uma **cópia completa** da chain. Não existe um servidor central — o ledger é o consenso entre todos os nós.

---

## O que é Blockchain? — Componentes

| Componente | O que é | No nosso sistema |
|---|---|---|
| **Bloco** | Agrupamento de transações + hash do anterior | Produzido a cada 5s pelos sealers |
| **Transação** | Chamada autenticada a uma função do contrato | `requestDrone()`, `transfer()` |
| **Estado** | Variáveis atuais do contrato | `balances[empresa_a] = 460` |
| **Evento (Log)** | Registro imutável emitido pelo contrato | `DroneRequested`, `DroneDispatched` |
| **Conta** | Endereço + chave privada ECDSA | Cada empresa e setor tem a sua |
| **Nó** | Computador que mantém a chain | 1 por setor = 4 nós |
| **Consensus** | Regra para todos concordarem com a chain | Clique PoA |

---

## Escolha Tecnológica

Por que **Ethereum (geth) PoA privado** e não outras opções?

| Opção | Vantagens | Desvantagens |
|---|---|---|
| **geth Clique PoA ✓** | EVM completa, Solidity, web3.py maduro, sem mineração pesada | Requer configuração de genesis e sealers |
| Ganache/Hardhat | Fácil de configurar | **Centralizado** — servidor único, não descentralizado |
| Ethereum testnet (Sepolia) | Sem infraestrutura própria | Requer internet, faucet, latência alta |
| Hyperledger Fabric | Ótimo para consórcios | Infraestrutura complexa (MSP, CA, orderer) |
| Blockchain própria | Controle total | Reinventar consenso distribuído — enorme esforço |
| IOTA (Tangle) | Sem taxas | DAG, menor suporte a contratos complexos |

<div class="rubric">
☑ Rubric: "por que escolheu essa tecnologia? Quais os trade-offs?" — geth PoA oferece o melhor equilíbrio entre descentralização real, maturidade da toolchain e complexidade de operação.
</div>

---

## Rede Clique PoA — Teoria do Consenso

<div class="theory">

**Proof of Authority (PoA)** — Em vez de competir por hash (PoW) ou por stake (PoS), um conjunto pré-autorizado de **sealers** assina blocos em rodízio. Adequado para redes de consórcio onde os participantes são conhecidos.

</div>

**Algoritmo Clique (EIP-225):**

```
Sealers: [S1, S2, S3, S4]   período: 5 segundos

Bloco 1:  vez de S1 → difficulty=2 (in-turn),  assina e propaga
Bloco 2:  vez de S2 → difficulty=2 (in-turn)
Bloco 3:  vez de S3 → difficulty=2 (in-turn)
...
```

- **In-turn** (vez do sealer): `difficulty = 2`
- **Out-of-turn** (fora da vez): `difficulty = 1` (permitido, mas menos preferido)
- **Resolução de forks:** chain com maior dificuldade acumulada vence → fork resolvido automaticamente

**Tolerância a falhas com N=4:**
```
Threshold = ⌊(N-1)/2⌋ = ⌊3/2⌋ = 1 nó pode cair
Mínimo para produzir blocos = N - threshold = 3 sealers ativos
```

---

## Rede Clique PoA — Arquitetura dos Nós

```
         devp2p TCP :30303 (P2P Ethereum)
         
geth_setor_1 ────────────── geth_setor_2
    │  │                        │  │
    │  └────────────────────────┘  │
    │                              │
geth_setor_3 ────────────── geth_setor_4

Cada nó:  cópia completa do ledger
          sealer autorizado (assina blocos)
          carteira do gerenciador de setor
          HTTP RPC :8545 (para web3.py)
```

**Propagação de transações:**
1. Empresa assina `requestDrone()` e envia ao nó local
2. Nó local valida a assinatura e propaga via **devp2p** para os outros 3 nós
3. O sealer da vez inclui a transação no próximo bloco
4. Bloco é propagado → todos os nós atualizam o estado

<div class="rubric">
☑ Rubric: "Verificar como novas transações e blocos são propagados" → protocolo devp2p nativo do Ethereum
</div>

---

## Contrato Inteligente: DroneToken.sol

```solidity
contract DroneToken {
    address public owner;
    mapping(address => uint256) public balances;  // saldo derivado do ledger

    // ─── Custo por criticidade ───────────────────────────────────
    function costFor(uint8 criticality) public pure returns (uint256) {
        if (criticality >= 4) return 40;  // bloqueio de rota, deriva...
        if (criticality == 3) return 30;  // falha sinalização, urgente...
        if (criticality == 2) return 20;  // objeto não identificado
        return 10;                         // inspeção rotineira
    }

    // ─── Empresa solicita drone (debita tokens atomicamente) ─────
    function requestDrone(uint8 sector, string calldata occType,
                          uint8 criticality, string calldata reqId) external {
        uint256 cost = costFor(criticality);
        require(balances[msg.sender] >= cost, "saldo insuficiente");
        balances[msg.sender] -= cost;
        emit DroneRequested(msg.sender, sector, occType, criticality, cost, reqId, block.timestamp);
    }

    // ─── Transferência entre empresas ────────────────────────────
    function transfer(address to, uint256 amount) external {
        require(balances[msg.sender] >= amount, "saldo insuficiente");
        balances[msg.sender] -= amount;
        balances[to]          += amount;
        emit Transfer(msg.sender, to, amount);
    }
}
```

<div class="rubric">
☑ Rubric: saldo derivado do ledger (não banco de dados local) — `balances` é variável de estado do contrato EVM
</div>

---

## Contrato: Eventos (Log Imutável)

Todos os eventos registrados no contrato ficam na chain para sempre:

```solidity
// Empresa → solicita drone (com pagamento)
event DroneRequested(address indexed requester, uint8 indexed sector,
                     string occurrenceType, uint8 criticality,
                     uint256 cost, string requestId, uint256 ts);

// Gerenciador → despacha drone
event DroneDispatched(uint8 indexed sector, string occurrenceId,
                      string droneId, string requestId, uint256 ts);

// Gerenciador → drone falhou em missão
event DroneFailed(uint8 indexed sector, string occurrenceId,
                  string droneId, uint256 ts);

// Gerenciador → missão concluída, drone retornou
event DroneRecalled(uint8 indexed sector, string droneId,
                    string occurrenceId, uint256 ts);

// Empresa → transferiu tokens
event Transfer(address indexed from, address indexed to, uint256 amount);
```

Cada evento é armazenado no **receipt** da transação → imutável por hash encadeado.

<div class="rubric">
☑ Rubric: "laudo registrado no ledger ao final de cada missão, com informações relevantes (drone, rota, resultado, data/hora)"
</div>

---

## Gestão de Ativos — Tokens Operacionais

**Como os créditos são emitidos:**

```python
# deploy.py — executado uma única vez após o deploy
contract.functions.mint(EMPRESA_A_ADDR, 500).build_transaction(...)
contract.functions.mint(EMPRESA_B_ADDR, 500).build_transaction(...)
```

Evento gerado: `Transfer(from=0x0, to=empresa_a, amount=500)` — mint registrado como Transfer com `from` = endereço zero.

**Custo por tipo de ocorrência:**

| Criticidade | Exemplos | Custo |
|---|---|---|
| 4 — máxima | Bloqueio de rota, embarcação à deriva, risco ambiental | 40 tokens |
| 3 — alta | Falha de sinalização, congestionamento, inspeção urgente | 30 tokens |
| 2 — média | Objeto não identificado | 20 tokens |
| 1 — baixa | Inspeção rotineira | 10 tokens |

**Autenticação das transferências:**
Cada transação é **assinada com ECDSA** pela chave privada da empresa. A EVM verifica que `msg.sender == dono_do_saldo` antes de executar qualquer débito.

<div class="rubric">
☑ Rubric: "transferências autenticadas — assinatura digital/chaves identificando o dono do ativo"
</div>

---

## Camada IoT — MQTT Implementado do Zero

**Por que MQTT alongside blockchain?**

A blockchain confirma pagamentos em ~5 segundos (tempo de bloco). O MQTT opera em milissegundos. São camadas com propósitos distintos:

| Camada | Tecnologia | Latência | Propósito |
|---|---|---|---|
| Blockchain | geth + Solidity | ~5s | Pagamento, registro imutável |
| IoT | MQTT (TCP) | <100ms | Despacho físico do drone |
| Exclusão Mútua | Ricart-Agrawala (TCP) | <50ms | Acesso exclusivo ao drone |

**Protocolo MQTT implementado do zero** (sem bibliotecas):

```python
# Pacote CONNECT codificado manualmente
cid = client_id.encode()
var = b"\x00\x04MQTT\x04\x02\x00\x3c" + bytes([len(cid)>>8, len(cid)&0xFF]) + cid
sock.sendall(bytes([0x10]) + encode_remaining_length(len(var)) + var)

# Remaining Length: codificação de comprimento variável MQTT 3.1.1
# 7 bits por byte; bit 7 indica continuação
```

<div class="rubric">
☑ Rubric: "Caso o aluno tenha mantido o consenso do problema anterior, arguir por que" → RA resolve exclusão física em ms; blockchain resolve double-spend em segundos
</div>

---

## Ricart-Agrawala + Relógio de Lamport

**Por que Ricart-Agrawala ainda é necessário?**

A blockchain serializa transações de tokens, mas **não controla qual drone físico** cada gerenciador usa. Dois gerenciadores com tokens suficientes poderiam despachar o mesmo drone simultaneamente sem RA.

**Algoritmo:**

```
SM1 quer drone_a → envia REQUEST(ts=5, crit=4, sector=1) para SM2, SM3, SM4
SM2 não está disputando drone_a → responde REPLY imediatamente
SM3 disputa drone_a com ts=3 (anterior) → adia REPLY (tem prioridade)
SM4 disputa drone_a com crit=3 (menor) → responde REPLY imediatamente

Critério de prioridade:
  1º) criticidade MAIOR tem prioridade
  2º) se igual: timestamp Lamport MENOR
  3º) se igual: sector_id MENOR (desempate determinístico)
```

**Relógio de Lamport:**

```python
# Ao enviar evento:     tick()   → self._t += 1
# Ao receber mensagem: update() → self._t = max(local, recebido) + 1
```

Garante ordenação causal entre eventos distribuídos mesmo sem clock global.

---

## Fluxo Completo de uma Solicitação

```
EMPRESA A                  BLOCKCHAIN               SECTOR MANAGER 1          DRONE_A
    │                          │                           │                      │
    │ 1. requestDrone(1,       │                           │                      │
    │    "bloqueio", 4, reqId) │                           │                      │
    │──────────────────────────►                           │                      │
    │                    [EVM executa:]                    │                      │
    │                    balances[A] -= 40                 │                      │
    │                    emit DroneRequested               │                      │
    │◄──── tx confirmado (~5s) ─┤                          │                      │
    │                          │                           │                      │
    │ 2. MQTT manual_request   │                           │                      │
    │    {type, request_id}    │                           │                      │
    │──────────────────────────────────────────────────────►                      │
    │                          │       [RA REQUEST → SM2, SM3, SM4]               │
    │                          │       [aguarda REPLY de todos]                   │
    │                          │       [adquire drone_a]                          │
    │                          │                           │                      │
    │                          │           3. MQTT dispatch │─────────────────────►
    │                          │           4. recordDispatch│                      │
    │                          │────────────────────────────►                     │
    │                          │       DroneDispatched event│   [missão em curso] │
    │                          │           5. recordRecall  │◄────────────────────│
    │                          │────────────────────────────►                     │
```

---

## Prevenção de Duplo Gasto — Teoria

<div class="theory">

**Duplo gasto** ocorre quando o mesmo saldo é usado para duas transações diferentes. Em sistemas centralizados, um banco serializa transações. Em sistemas descentralizados, a serialização é garantida pelo **consenso**.

</div>

**Como o Ethereum previne:**

```
Bloco N: [tx1: empresa_A gasta 40 tokens]  ← incluída primeiro
Bloco N: [tx2: empresa_A gasta 40 tokens]  ← segunda tx no mesmo bloco

Execução sequencial pela EVM:
  tx1: balances[A]=500 → require(500>=40) ✓ → balances[A]=460 → emit
  tx2: balances[A]=460 → require(460>=40) ✓ → balances[A]=420 → emit
  (ambas passam se saldo for suficiente)
```

```
Bloco N: [tx1: empresa_A gasta 40 tokens]
Bloco M: [tx2: empresa_A gasta 40 tokens]  ← saldo=10 após bloco N

  tx2: balances[A]=10 → require(10>=40) ✗ → REVERT "saldo insuficiente"
```

**Em que ponto é detectado?** Durante a **execução da transação pelo EVM**, antes da inclusão definitiva no bloco — não apenas na interface do cliente.

<div class="rubric">
☑ Rubric: "validação do saldo é feita contra o estado do ledger antes da inclusão" — EVM executa `require` contra o estado atual do contrato
</div>

---

## Prevenção de Duplo Gasto — Implementação

```python
# client.py — empresa só envia MQTT após confirmação blockchain

def request_drone(self, sector, occ_type, criticality):
    tx = self._contract.functions.requestDrone(
        sector, occ_type, criticality, request_id
    ).build_transaction({
        "from":     self._addr,
        "nonce":    self._nonce,   # nonce impede replay attacks
        "gas":      150_000,
        "gasPrice": self._w3.eth.gas_price,
    })
    signed  = self._w3.eth.account.sign_transaction(tx, self._key)
    tx_hash = self._w3.eth.send_raw_transaction(signed.rawTransaction)
    
    # AGUARDA confirmação — só continua se a tx entrou num bloco
    self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    return tx_hash.hex()   # request_id enviado no MQTT

# Drone só é despachado se request_id (tx_hash) vier no manual_request
```

**Nonce:** cada conta tem um contador de transações (nonce). Transações com nonce repetido são rejeitadas → previne replay attacks.

---

## Log Imutável — Como Funciona na Prática

**Gerenciador registra cada evento no contrato:**

```python
# sector_manager.py — BlockchainLogger (fire-and-forget em thread separada)

def record_dispatch(self, sector, occ_id, drone_id, request_id):
    self._enqueue(self._contract.functions.recordDispatch,
                  sector, occ_id, drone_id, request_id)

def record_drone_failed(self, sector, occ_id, drone_id):
    self._enqueue(self._contract.functions.recordDroneFailed,
                  sector, occ_id, drone_id)

def record_recall(self, sector, drone_id, occ_id):
    self._enqueue(self._contract.functions.recordRecall,
                  sector, drone_id, occ_id)
```

**Saída do audit.py:**

```
  [2026-06-17 22:05:01 UTC] MINT         empresa_a  +500 tokens
  [2026-06-17 22:07:43 UTC] SOLICITADO   setor=1  tipo=bloqueio_de_rota  crit=4  custo=40
  [2026-06-17 22:07:48 UTC] DESPACHADO   setor=1  occ=occ_s1_0001  drone=drone_a
  [2026-06-17 22:09:10 UTC] RETORNO      setor=1  drone=drone_a  occ=occ_s1_0001
```

<div class="rubric">
☑ Rubric: "verificar se o laudo é registrado com informações relevantes (drone, rota, resultado, data/hora)"
</div>

---

## Transparência e Auditabilidade

**Qualquer participante pode consultar sem permissões especiais:**

```bash
# Auditoria via nó do setor 1
python3 blockchain/audit.py \
  --url http://localhost:18541 \
  --contract 0x<CONTRACT_ADDR>

# Mesma consulta via nó do setor 3 → resultado IDÊNTICO
python3 blockchain/audit.py \
  --url http://localhost:18543 \
  --contract 0x<CONTRACT_ADDR>
```

**Consulta direta via JSON-RPC (sem ferramentas):**

```bash
# Número de bloco atual
curl -s http://localhost:18541 \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  -H 'Content-Type: application/json'

# Peers conectados (descentralização)
curl -s http://localhost:18541 \
  -d '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}' \
  -H 'Content-Type: application/json'
```

<div class="rubric">
☑ Rubric: "Consultar o mesmo dado a partir de dois nós distintos e verificar a consistência das respostas"
</div>

---

## Descentralização — Prova Concreta

**4 nós independentes, cada um com cópia completa:**

```
geth_setor_1  ←─ devp2p ─→  geth_setor_2
     ↕                            ↕
geth_setor_3  ←─ devp2p ─→  geth_setor_4

Cada nó:
  ✓ mantém chaindata completa em volume Docker
  ✓ é sealer autorizado no genesis.json
  ✓ propaga blocos e transações independentemente
  ✗ não existe nó mestre ou banco central de saldos
```

**Teste de queda de nó ao vivo:**

```bash
# Derrubar o nó do setor 4
docker stop geth_setor_4

# Chain continua (3 de 4 sealers = acima do threshold)
# threshold = ⌊(4-1)/2⌋ = 1 → mínimo = 3 ativos

# Verificar que blocos continuam sendo produzidos:
curl -s http://localhost:18541 \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  -H 'Content-Type: application/json'

# Restaurar — sincroniza automaticamente:
docker start geth_setor_4
```

<div class="rubric">
☑ Rubric: "Derrubar um dos nós durante a demonstração e verificar se o sistema continua operando"
</div>

---

## Estrutura da Rede — genesis.json

```json
{
  "config": {
    "chainId": 1337,
    "clique": { "period": 5, "epoch": 30000 }
  },
  "extradata": "0x[32 zeros][sealer1][sealer2][sealer3][sealer4][65 zeros]",
  "alloc": {
    "0x<sealer_1>": { "balance": "1000000000000000000000" },
    "0x<empresa_a>": { "balance": "1000000000000000000000" },
    ...
  }
}
```

- `extradata` = lista de sealers autorizados (endereços ECDSA, ordenados)
- `alloc` = ETH inicial para pagar gas (não confundir com tokens DroneToken)
- `chainId: 1337` = rede privada (diferencia de mainnet, testnets)
- `period: 5` = um bloco a cada 5 segundos

**Geração determinística das contas:**

```bash
python3 blockchain/geth/genkeys.py
# → gera .env com endereços e chaves privadas
# → atualiza genesis.json com os novos sealers
# → calcula endereço do contrato (antes do deploy)
```

---

## Endereço Determinístico do Contrato

Um ponto importante: **o endereço do contrato é calculado antes do deploy**, eliminando a necessidade de distribuí-lo via MQTT.

**Fórmula (CREATE opcode):**

```
CONTRACT_ADDR = keccak256(rlp([deployer_address, nonce=0]))[últimos 20 bytes]
```

```python
# genkeys.py
def contract_address(deployer, nonce=0):
    encoded = rlp.encode([bytes.fromhex(deployer[2:]), nonce])
    return "0x" + Web3.keccak(encoded).hex()[-40:]
```

**Consequência:** todos os serviços (sector_managers, empresas) recebem `CONTRACT_ADDR` via variável de ambiente no docker-compose. Não há comunicação centralizada para distribuir o endereço.

---

## Comparação: Problema 2 → Problema 3

| Aspecto | Problema 2 | Problema 3 |
|---|---|---|
| **Exclusão mútua** | Ricart-Agrawala | Ricart-Agrawala (mantido) |
| **Saldo de créditos** | Não existia | DroneToken.sol no ledger |
| **Pré-condição do despacho** | Nenhuma | Pagamento confirmado na chain |
| **Registro de operações** | Apenas logs locais | Eventos Solidity imutáveis |
| **Ledger distribuído** | Não existia | 4 nós geth Clique PoA |
| **Auditabilidade** | Impossível externamente | Qualquer nó, sem permissão |
| **Tolerância a falhas** | RA timeout 6s | RA timeout + Clique N-1 |
| **Autenticação** | Nenhuma (MQTT aberto) | ECDSA por transação |

<div class="warning">

**Decisão de projeto:** O RA foi mantido porque resolve exclusão física em milissegundos — a blockchain leva ~5s por bloco. Usar apenas blockchain implicaria wait time de bloco antes do despacho físico, inaceitável para emergências.

</div>

---

## Roteiro de Demonstração

### 1. Verificar descentralização
```bash
for p in 18541 18542 18543 18544; do
  echo -n "Nó :$p → peers: "
  curl -s http://localhost:$p -H 'Content-Type:application/json' \
    -d '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}' \
    | python3 -c "import json,sys; print(int(json.load(sys.stdin)['result'],16))"
done
```

### 2. Consultar saldos e histórico
```bash
python3 blockchain/audit.py --url http://localhost:18541 --contract $CONTRACT
python3 blockchain/audit.py --url http://localhost:18543 --contract $CONTRACT
# → resultado idêntico em nós diferentes
```

### 3. Solicitar drone (empresa A)
```bash
docker compose run --rm -it empresa_a
# → escolher setor → escolher ocorrência → ver débito de tokens
```

---

## Roteiro de Demonstração (cont.)

### 4. Transferência entre empresas
```bash
# No terminal da empresa A:
docker compose run --rm -it empresa_a
# → opção [t] → endereço empresa B → quantidade
# Confirmar em nó diferente:
python3 blockchain/audit.py --url http://localhost:18543 --contract $CONTRACT
```

### 5. Teste de duplo gasto
```bash
# Dois terminais empresa A com saldo = 40
docker compose run --rm -it empresa_a   # terminal 1 → solicitar crit=4
docker compose run --rm -it empresa_a   # terminal 2 → solicitar crit=4 simultaneamente
# → uma é confirmada, a outra recebe "saldo insuficiente"
```

### 6. Teste de queda de nó
```bash
docker stop geth_setor_4          # derruba 1 de 4 nós
sleep 10
# verificar que blocos continuam:
curl -s http://localhost:18541 -d '{"jsonrpc":"2.0","method":"eth_blockNumber",...}'
docker start geth_setor_4         # restaura — sincroniza automaticamente
```

---

## Resumo — Atendimento ao Rubric

| Critério | ✓ | Como atendido |
|---|---|---|
| **Descentralização** | ✓ | 4 nós geth independentes, sem banco central, cada um com ledger completo |
| **Sem ponto único de falha** | ✓ | Clique tolera 1 nó offline; RA tolera peer silencioso |
| **Comunicação P2P** | ✓ | devp2p entre nós geth; MQTT (raw TCP) para IoT |
| **Consenso explicável** | ✓ | Clique PoA: sealers em rodízio, difficulty 2 vs 1, fork = chain mais pesada |
| **Gestão de ativos** | ✓ | DroneToken.sol; saldo no ledger, não variável local |
| **Transferências autenticadas** | ✓ | ECDSA por transação; `msg.sender` verificado pelo EVM |
| **Prevenção de duplo gasto** | ✓ | `require(balance >= cost)` na EVM antes da inclusão no bloco |
| **Pagamento como pré-condição** | ✓ | `wait_for_transaction_receipt()` antes do MQTT |
| **Log imutável** | ✓ | Eventos Solidity em cada bloco; hash encadeado garante imutabilidade |
| **Auditabilidade pública** | ✓ | `audit.py` em qualquer nó, sem permissão; JSON-RPC aberto |

---

<!-- _paginate: false -->

# Obrigada!

**Repositório:** `P03_block_chain` — branch `main`

**Stack:**
- Python 3.9 · geth v1.13.15 · Solidity 0.8 · web3.py 6.x
- Docker Compose · MQTT (raw socket) · Ricart-Agrawala · Lamport Clock

**Referências:**
- EIP-225: Clique Proof-of-Authority Consensus Protocol
- Ricart, Agrawala (1981) — Mutual Exclusion in Computer Networks
- Lamport (1978) — Time, Clocks, and the Ordering of Events
- go-ethereum v1.13.15 — https://geth.ethereum.org/
- web3.py — https://web3py.readthedocs.io/
