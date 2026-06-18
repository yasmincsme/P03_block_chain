<div align="center">

# Sistema de Monitoramento do Estreito Marítimo com Blockchain Integrada

#### Projeto da disciplina TEC 502 - Concorrência e Conectividade

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-306998?logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![MQTT](https://img.shields.io/badge/MQTT-660066?logoColor=white)](https://mqtt.org/)
[![Ethereum](https://img.shields.io/badge/Ethereum-3C3C3D?logo=ethereum&logoColor=white)](https://ethereum.org/)
[![Solidity](https://img.shields.io/badge/Solidity-363636?logo=solidity&logoColor=white)](https://soliditylang.org/)

</div>

> Sistema distribuído de monitoramento marítimo com **blockchain Ethereum privada**. Quatro setores operam com seus próprios brokers MQTT, gerenciadores, drones e nós **geth** (go-ethereum). O algoritmo de **Ricart-Agrawala** garante exclusão mútua na alocação física de drones. O contrato **DroneToken** (EVM/Solidity) controla os créditos operacionais e registra todas as operações de forma imutável. A rede Clique **PoA** com 4 sealers garante descentralização real — cada setor mantém sua própria cópia completa do ledger.

---

## Sumário

- [Introdução](#introdução)
- [Tecnologias e Ferramentas](#tecnologias-e-ferramentas)
- [Funcionalidades](#funcionalidades)
- [Arquitetura do Sistema](#arquitetura-do-sistema)
  - [Camada IoT (MQTT)](#camada-iot-mqtt)
  - [Camada Blockchain (Ethereum)](#camada-blockchain-ethereum)
- [Componentes](#componentes)
- [Blockchain — Rede Clique PoA](#blockchain--rede-clique-poa)
  - [Consenso e Descentralização](#consenso-e-descentralização)
  - [Contrato DroneToken](#contrato-dronetoken)
  - [Fluxo de Pagamento](#fluxo-de-pagamento)
- [Algoritmo de Exclusão Mútua](#algoritmo-de-exclusão-mútua-ricart-agrawala)
- [Como Utilizar](#como-utilizar)
  - [Pré-requisitos](#pré-requisitos)
  - [Geração das Chaves](#1-geração-das-chaves-uma-única-vez)
  - [Inicialização dos Serviços](#2-inicialização-dos-serviços)
  - [Interação com o Sistema](#3-interação-com-o-sistema)
- [Roteiro de Demonstração](#roteiro-de-demonstração)
  - [Descentralização](#descentralização)
  - [Comunicação P2P](#comunicação-p2p)
  - [Gestão de Ativos](#gestão-de-ativos-créditos)
  - [Prevenção de Duplo Gasto](#prevenção-de-duplo-gasto)
  - [Requisição e Pagamento](#requisição-e-pagamento-de-escoltas)
  - [Log Imutável](#log-de-operações-imutável)
  - [Transparência e Auditabilidade](#transparência-e-auditabilidade)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Equipe](#equipe)
- [Referências](#referências)

---

## Introdução

O sistema simula o monitoramento de um estreito marítimo dividido em **quatro setores geográficos**. Cada setor opera autonomamente com um broker MQTT dedicado e um nó Ethereum independente.

Os drones são um **recurso físico compartilhado**: qualquer gerenciador pode despachar qualquer drone — mas o despacho só ocorre após duas condições serem satisfeitas:

1. **Pagamento confirmado na blockchain** — o cliente chama `requestDrone()` no contrato `DroneToken`, que debita os tokens atomicamente. O drone só é alocado depois da transação ser incluída num bloco.
2. **Exclusão mútua distribuída** — o algoritmo de **Ricart-Agrawala** garante que apenas um gerenciador por vez acessa o mesmo drone físico.

Toda operação relevante (solicitação, despacho, falha, retorno do drone) é registrada como evento Solidity na blockchain — imutável por construção do hash encadeado.

---

## Tecnologias e Ferramentas

| Tecnologia | Uso no projeto |
|---|---|
| **Python 3.9** | Todos os componentes de aplicação |
| **Socket TCP/UDP** | MQTT implementado do zero, sem bibliotecas externas |
| **Threading** | Concorrência dentro de cada componente |
| **Curses** | Interface TUI do monitor |
| **JSON** | Serialização de todas as mensagens |
| **Docker / Compose** | Orquestração dos ~20 containers |
| **geth v1.13.15** | Nós Ethereum — rede Clique PoA privada |
| **Solidity 0.8** | Contrato `DroneToken.sol` |
| **web3.py 6.x** | Interface Python ↔ Ethereum |
| **py-solc-x** | Compilação de Solidity em Python |

> **Por que geth v1.13.15?** Versões ≥ 1.14 exigem um cliente de consenso PoS separado (pós-merge). A v1.13.15 é a última que suporta Clique (PoA) standalone — ideal para redes privadas sem infraestrutura adicional.

---

## Funcionalidades

- **Blockchain privada descentralizada:** 4 nós geth independentes, cada um com cópia completa do ledger. Sem nó mestre, sem banco de dados central de saldos.
- **Token de crédito operacional (DroneToken):** emissão via `mint()`, consumo via `requestDrone()`, transferência via `transfer()`. Saldo derivado do histórico de transações no ledger.
- **Prevenção de duplo gasto:** a validação do saldo é feita pelo EVM dentro do consenso. Duas transações competindo pelo mesmo saldo em blocos diferentes são serializadas — a segunda falha com `"saldo insuficiente"`.
- **Pagamento como pré-condição do despacho:** o cliente paga em tokens antes de enviar o comando MQTT. Sem confirmação na chain, o drone não é despachado.
- **Log imutável de operações:** `DroneRequested`, `DroneDispatched`, `OccurrenceRequeued`, `DroneFailed`, `DroneRecalled` — todos registrados como eventos Solidity. Modificar um bloco invalida todos os subsequentes.
- **Exclusão mútua distribuída (RA):** Ricart-Agrawala com relógio de Lamport e prioridade por criticidade resolve conflitos de alocação física entre gerenciadores.
- **Auditoria pública:** qualquer participante consulta saldos, transações e laudos via `audit.py` em qualquer nó, sem permissões especiais.
- **Tolerância a falhas:** Clique com N=4 sealers tolera 1 nó offline (threshold: ⌊(4−1)/2⌋=1). Peers RA silenciosos são tratados como respondidos após timeout.

---

## Arquitetura do Sistema

O sistema opera em duas camadas independentes que se complementam:

```
┌─────────────────────────────────────────────────────────────┐
│                    CAMADA BLOCKCHAIN                        │
│                                                             │
│  geth_1 ←──devp2p──→ geth_2 ←──devp2p──→ geth_3          │
│    ↑                                           ↕            │
│  geth_4 ←──────────────────────────────────────┘           │
│                                                             │
│  (cada nó = cópia completa do ledger, sealer Clique)       │
│  (DroneToken.sol implantado — mesmo estado em todos os nós) │
└────────────────────┬────────────────────────────────────────┘
                     │ web3.py (HTTP RPC)
┌────────────────────▼────────────────────────────────────────┐
│                    CAMADA IoT (MQTT)                        │
│                                                             │
│  SM1 ←─RA TCP :5001─→ SM2 ←─→ SM3 ←─→ SM4               │
│   │                    │         │         │                │
│  B1                   B2        B3        B4  (brokers)    │
│   │                    │         │         │                │
│  Da Db               Dc Dd     De Df     Dg Dh (drones)   │
│                                                             │
│  empresa_a ──MQTT──→ broker_1 ──→ SM1 ──→ drone dispatch  │
└─────────────────────────────────────────────────────────────┘
```

### Camada IoT (MQTT)

| Conexão | Protocolo | Detalhe |
|---|---|---|
| Drone → Broker | TCP MQTT | Status com `retain=True` |
| Gerenciador → Broker local | TCP MQTT | Publica ocorrências e despachos |
| Gerenciador → Todos os brokers | TCP MQTT | Monitora status de todos os 8 drones |
| Gerenciador ↔ Gerenciador | TCP direto :5001 | Ricart-Agrawala (bypassa brokers) |
| Empresa → Broker | TCP MQTT | `manual_request` após pagamento |
| Monitor → Todos os brokers | TCP MQTT | Leitura de todos os tópicos |

### Camada Blockchain (Ethereum)

| Conexão | Protocolo | Detalhe |
|---|---|---|
| Nó ↔ Nó | devp2p (TCP :30303) | Propagação de blocos e transações |
| Empresa → Nó geth | HTTP RPC :8545 | Chama `requestDrone()`, `transfer()` |
| Gerenciador → Nó geth | HTTP RPC :8545 | Registra dispatch, falha, recall |
| Auditoria → Qualquer nó | HTTP RPC :8545 | Consulta saldos e eventos |

**Distribuição dos serviços por setor:**

| Setor | Broker | Nó geth | Drones | Gerenciador |
|---|---|---|---|---|
| S1 | broker_setor_1 :3001 | geth_setor_1 :18541 | drone_a, drone_b | setor_1_manager |
| S2 | broker_setor_2 :3002 | geth_setor_2 :18542 | drone_c, drone_d | setor_2_manager |
| S3 | broker_setor_3 :3003 | geth_setor_3 :18543 | drone_e, drone_f | setor_3_manager |
| S4 | broker_setor_4 :3004 | geth_setor_4 :18544 | drone_g, drone_h | setor_4_manager |

---

## Componentes

### Broker MQTT
`broker/iot_broker.py` — Implementação própria do protocolo MQTT. Escuta TCP e UDP na mesma porta. Suporta QoS 0 e 1, wildcards (`+`, `#`) e retained messages.

### Gerenciador de Setor
`sector_manager/sector_manager.py` — Orquestra o setor:
1. Recebe `manual_request` da empresa (via MQTT) — apenas se a transação blockchain vier no payload.
2. Enfileira ocorrências por criticidade, timestamp Lamport e sector_id.
3. Executa Ricart-Agrawala para adquirir exclusividade sobre o drone.
4. Despacha o drone via MQTT e registra o evento no contrato (`recordDispatch`).
5. Monitora a missão; em caso de falha registra `recordDroneFailed` + `recordRequeue` e reenfileira.
6. Ao concluir, registra `recordRecall`.

Conecta-se ao **nó geth do seu setor** via web3.py (HTTP RPC) e assina transações com a chave privada do sealer correspondente.

### Drone
`drone/drone_agent.py` — Publica status (`available` / `busy` / `offline`) com `retain=True`. Aguarda comandos `dispatch` e `recall`.

### Cliente Empresa
`client/client.py` — Terminal interativo que:
- Exibe o saldo atual de tokens (consultado diretamente no ledger).
- Cobra o pagamento em tokens **antes** de enviar o MQTT: chama `requestDrone()`, aguarda confirmação do bloco e só então publica `manual_request` com o `tx_hash` como `request_id`.
- Permite transferir tokens para outra empresa via `transfer()`.

### Monitor TUI
`monitor/monitor.py` — Interface curses com atualização a cada 400ms. Mostra status dos 4 setores, 8 drones e últimos eventos.

---

## Blockchain — Rede Clique PoA

### Consenso e Descentralização

A rede usa **Clique (Proof of Authority)** — consenso Ethereum para redes privadas onde sealers pré-autorizados assinam blocos em rodízio.

- **4 sealers** = 1 por setor. Cada nó geth é independente e mantém uma cópia completa do ledger.
- **Tolerância a falhas:** com N=4 sealers, o sistema tolera ⌊(4−1)/2⌋=1 nó offline. Com 3 sealers ativos, a chain continua produzindo blocos a cada 5s.
- **Sem nó mestre:** qualquer nó pode receber transações e propagá-las. Não há autoridade central.
- **Resolução de forks:** se dois sealers produzirem blocos simultâneos, o Clique usa dificuldade 2 (in-turn) vs 1 (out-of-turn) para selecionar a chain mais pesada. O fork é resolvido automaticamente.

**Por que Ethereum e não IOTA ou Hyperledger?**
Ethereum/geth tem suporte nativo à EVM, permitindo usar contratos Solidity para lógica de tokens e eventos. A toolchain (web3.py, Solidity) é madura e bem documentada. Hyperledger exige infraestrutura adicional (MSP, CA, orderer). IOTA é voltado para DAG e tem menor suporte a contratos complexos. O geth PoA privado tem o melhor trade-off para redes de consórcio pequenas.

**Por que manter Ricart-Agrawala?**
O RA resolve exclusão mútua **física e em tempo real** — dois gerenciadores não podem acionar o mesmo drone simultaneamente. A blockchain resolve exclusão de **saldo** (double-spend). São camadas complementares que operam em escalas de tempo e abstrações diferentes (ms vs. segundos de bloco).

### Contrato DroneToken

`blockchain/DroneToken.sol` — Implantado via `blockchain/deploy.py`.

| Função | Chamada por | Efeito |
|---|---|---|
| `mint(to, amount)` | Deployer | Emite tokens (gênese e recarga) |
| `requestDrone(sector, type, crit, reqId)` | Empresa | Debita tokens e emite `DroneRequested` |
| `transfer(to, amount)` | Empresa | Transfere tokens entre empresas |
| `recordDispatch(sector, occId, droneId, reqId)` | Gerenciador | Emite `DroneDispatched` |
| `recordRequeue(sector, occId, reason)` | Gerenciador | Emite `OccurrenceRequeued` |
| `recordDroneFailed(sector, occId, droneId)` | Gerenciador | Emite `DroneFailed` |
| `recordRecall(sector, droneId, occId)` | Gerenciador | Emite `DroneRecalled` |

**Saldo derivado do ledger:** `balances[addr]` é uma variável de estado do contrato, não um banco de dados externo. O saldo de cada empresa é a soma das operações de mint e transfer registradas nos blocos — auditável por qualquer nó.

**Custo por criticidade:**

| Criticidade | Tipos de ocorrência | Custo |
|---|---|---|
| 4 (máxima) | Bloqueio de rota, embarcação à deriva, risco ambiental | 40 tokens |
| 3 | Falha de sinalização, congestionamento, inspeção urgente | 30 tokens |
| 2 | Objeto não identificado | 20 tokens |
| 1 | Inspeção rotineira | 10 tokens |

### Fluxo de Pagamento

```
Empresa                    Blockchain (geth)            MQTT              Gerenciador
   │                              │                       │                     │
   │── requestDrone(sector, ...) ─►                       │                     │
   │                    [EVM: valida saldo]                │                     │
   │                    [debita tokens]                    │                     │
   │                    [emite DroneRequested]             │                     │
   │◄── tx_hash (confirmado em ~5s) ──────────────────────│                     │
   │                                                       │                     │
   │── publish manual_request {type, request_id=tx_hash} ─►                     │
   │                                                       │── Ricart-Agrawala ──►
   │                                                       │                [adquire drone]
   │                                                       │◄── recordDispatch ──│
   │                                                       │    (na chain)        │
```

---

## Algoritmo de Exclusão Mútua: Ricart-Agrawala

Garante que dois gerenciadores nunca despachem o mesmo drone simultaneamente.

1. O gerenciador envia `REQUEST` para os outros 3 com `(timestamp_Lamport, criticidade, sector_id)`.
2. Cada peer responde com `REPLY` imediatamente, salvo se também está disputando o **mesmo drone** com prioridade maior.
3. **Prioridade:** criticidade mais alta → timestamp menor → sector_id menor.
4. Com todos os `REPLY` recebidos, o gerenciador adquire o drone.
5. Ao finalizar, envia `RELEASE` e entrega os `REPLY` adiados.

**Tolerância a falhas RA:** peers sem resposta em 6s são contabilizados como respondidos — o sistema não trava na ausência de um setor.

**Relógio de Lamport:** incrementado a cada evento local; atualizado para `max(local, recebido) + 1` ao receber mensagens.

---

## Como Utilizar

### Pré-requisitos

- Docker e Docker Compose instalados.
- Python 3.9+ com `pip install web3 eth-account eth-keys rlp` (apenas para gerar chaves).

### 1. Geração das Chaves (uma única vez)

O script `genkeys.py` cria todas as contas Ethereum, atualiza `genesis.json` e grava o arquivo `.env` na raiz do projeto:

```bash
cd /caminho/para/P03_block_chain
pip install web3 eth-account eth-keys rlp
python3 blockchain/geth/genkeys.py
```

O `.env` gerado contém os endereços e chaves de todos os sealers, deployer e empresas. O `docker-compose.yml` usa essas variáveis com `${VAR}`.

> **Segurança:** nunca commitar o arquivo `.env` em repositórios públicos.

### 2. Inicialização dos Serviços

```bash
# Brokers MQTT
docker compose up -d broker_1 broker_2 broker_3 broker_4

# Nós Ethereum (rede Clique PoA)
docker compose up -d geth_setor_1 geth_setor_2 geth_setor_3 geth_setor_4

# Aguarda ~30s para os nós se conectarem via P2P e começarem a minerar
# Verificar: curl -s http://localhost:18541 -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' -H 'Content-Type: application/json'

# Implanta o contrato DroneToken (executa uma única vez e termina)
docker compose up blockchain_deployer

# Gerenciadores de setor
docker compose up -d sector_manager_1 sector_manager_2 sector_manager_3 sector_manager_4

# Drones
docker compose up -d drone_a drone_b drone_c drone_d drone_e drone_f drone_g drone_h
```

### 3. Interação com o Sistema

```bash
# Terminal empresa A (paga tokens, solicita drones)
docker compose run --rm -it empresa_a

# Terminal empresa B (em outro terminal)
docker compose run --rm -it empresa_b

# Monitor TUI
docker compose run --rm -it monitor

# Auditoria (consulta saldos e histórico de qualquer nó)
python3 blockchain/audit.py \
  --url http://localhost:18541 \
  --contract $(grep CONTRACT_ADDR .env | cut -d= -f2)
```

**Logs dos serviços:**

```bash
docker logs -f setor_1_manager
docker logs -f geth_setor_1
```

**Encerramento:**

```bash
docker compose down
docker compose down -v   # remove também os volumes geth (apaga a chain)
```

---

## Roteiro de Demonstração

Esta seção mapeia cada critério do rubric ao teste correspondente no sistema.

---

### Descentralização

**Verificar múltiplos nós com cópia própria do ledger:**

```bash
# Número de peers de cada nó (deve ser 3 para um nó completo)
curl -s http://localhost:18541 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}'

curl -s http://localhost:18542 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}'

# Número de bloco deve ser idêntico em todos os nós
for port in 18541 18542 18543 18544; do
  echo -n "Nó :$port → bloco "
  curl -s http://localhost:$port -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
    | python3 -c "import json,sys; print(int(json.load(sys.stdin)['result'], 16))"
done
```

**Teste de queda de nó (tolerância a falhas):**

```bash
# Derrubar o nó do setor 4
docker stop geth_setor_4

# A chain deve continuar produzindo blocos (3 de 4 sealers ativos)
# Aguardar 10s e verificar que o bloco avançou
curl -s http://localhost:18541 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# Restaurar o nó — ele sincroniza automaticamente
docker start geth_setor_4
```

> Com N=4 sealers, o Clique exige ⌊N/2⌋+1 = 3 sealers ativos para produzir blocos. Um nó offline não para a rede.

---

### Comunicação P2P

**Verificar conectividade P2P entre os nós geth:**

```bash
# Lista de peers do nó 1
curl -s http://localhost:18541 -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"admin_peers","params":[],"id":1}' | python3 -m json.tool
```

**Mecanismo de consenso — Clique PoA:**

- Sealers pré-definidos no `extradata` do `genesis.json` assinam blocos em rodízio.
- Dificuldade 2 = vez do sealer (in-turn); dificuldade 1 = fora de vez (out-of-turn).
- Em caso de fork, a chain de maior dificuldade acumulada vence.
- Propagação de blocos e transações via protocolo **devp2p** (protocolo Ethereum nativo).

**Por que manter RA se há blockchain?**
O RA resolve em milissegundos quem acessa o drone físico agora. A blockchain confirma o pagamento em ~5s (tempo de bloco). São camadas complementares: o RA impede alocação física duplicada; o contrato impede gasto duplo de tokens. Resolver ambos apenas com blockchain implicaria wait time de bloco antes de acionar o drone fisicamente — inaceitável para despacho de emergência.

---

### Gestão de Ativos (Créditos)

**Verificar saldo inicial após deploy:**

```bash
CONTRACT=$(grep CONTRACT_ADDR .env | cut -d= -f2)
python3 blockchain/audit.py --url http://localhost:18541 --contract $CONTRACT
```

**Demonstrar transferência entre empresas:**

```bash
# No terminal da empresa A:
docker compose run --rm -it empresa_a
# Escolher opção [t] → transferir tokens → informar endereço da empresa B e quantidade
```

**Confirmar em nó diferente (consistência):**

```bash
# O saldo atualizado deve aparecer consultando o nó do setor 3
python3 blockchain/audit.py --url http://localhost:18543 --contract $CONTRACT
```

> O saldo não é uma variável local nem banco de dados externo — é derivado do histórico de transações `Transfer` e `DroneRequested` registradas nos blocos.

**Autenticação das transferências:** cada transação é assinada com a chave privada ECDSA da empresa. A EVM verifica a assinatura e associa o `msg.sender` ao saldo correto — impossível gastar tokens de outra empresa sem a chave privada.

---

### Prevenção de Duplo Gasto

**Como o sistema impede o duplo gasto:**

O contrato `requestDrone()` executa:
```solidity
require(balances[msg.sender] >= cost, "saldo insuficiente");
balances[msg.sender] -= cost;
emit DroneRequested(...);
```

A EVM serializa todas as transações num bloco. Se duas transações do mesmo remetente chegam com saldo suficiente apenas para uma, a segunda é rejeitada com `"saldo insuficiente"` — detectado **durante o consenso**, antes da inclusão definitiva no ledger.

**Teste prático de duplo gasto:**

```bash
# Em dois terminais simultâneos, solicitar dois drones com o mesmo saldo
# (empresa com saldo = 40, custo de cada requisição = 40)

# Terminal 1 — empresa A:
docker compose run --rm -it empresa_a   # solicitar ocorrência crit=4

# Terminal 2 — empresa A (segundo container):
docker compose run --rm -it empresa_a   # solicitar ao mesmo tempo
```

Apenas a transação incluída primeiro no bloco será aceita. A segunda retornará erro `"saldo insuficiente"` do contrato.

> **Em que ponto é detectado?** O Ethereum garante que cada bloco é processado serialmente. A segunda transação conflitante vê o estado **após** a primeira já ter sido aplicada — detectado no momento da execução dentro do consenso, não apenas na interface.

---

### Requisição e Pagamento de Escoltas

**Verificar que drone só é despachado após confirmação:**

O fluxo em `client/client.py`:
1. Chama `requestDrone()` e aguarda `wait_for_transaction_receipt()`.
2. **Só então** publica `manual_request` no MQTT com `request_id = tx_hash`.

Se a transação falha (saldo insuficiente, geth offline), o cliente exibe erro e **não envia o MQTT**.

**Testar companhia sem saldo suficiente:**

```bash
# No terminal da empresa (com saldo < custo da ocorrência)
docker compose run --rm -it empresa_a
# O cliente exibirá "Saldo insuficiente (X tokens, necessário Y)" antes mesmo de tentar a tx
```

**Teste de concorrência (duas empresas, mesmo drone):**

```bash
# Terminal 1 — empresa A solicita drone no setor 1
docker compose run --rm -it empresa_a   # setor 1, qualquer ocorrência

# Terminal 2 — empresa B solicita drone no setor 1 ao mesmo tempo
docker compose run --rm -it empresa_b   # setor 1, qualquer ocorrência
```

O Ricart-Agrawala garante que apenas um gerenciador despacha o drone. A segunda solicitação aguarda a liberação (ou reenfileira para outro drone).

---

### Log de Operações Imutável

**Verificar laudos registrados após missões:**

```bash
python3 blockchain/audit.py --url http://localhost:18541 --contract $CONTRACT
```

A saída lista todos os eventos em ordem cronológica: `SOLICITADO`, `DESPACHADO`, `FALHA`, `REENFILEIRADO`, `RETORNO`.

**O que garante a imutabilidade?**

Cada bloco contém o `hash` do bloco anterior (`parentHash`). Alterar qualquer dado em um bloco invalida seu hash, o que invalida o hash de todos os blocos subsequentes. Os outros 3 nós rejeitariam a chain adulterada porque seu hash acumulado seria menor que o da chain legítima.

**Teste de adulteração (conceitual):**

Para adulterar um registro, um atacante precisaria recalcular os hashes de todos os blocos posteriores **e** ter mais de 50% dos sealers — o que é inviável nesta rede de 4 nós onde os sealers são os próprios setores.

---

### Transparência e Auditabilidade

**Consultar dados a partir de dois nós distintos:**

```bash
CONTRACT=$(grep CONTRACT_ADDR .env | cut -d= -f2)

# Auditoria via nó do setor 1
python3 blockchain/audit.py --url http://localhost:18541 --contract $CONTRACT

# Mesma consulta via nó do setor 3 — resultado idêntico
python3 blockchain/audit.py --url http://localhost:18543 --contract $CONTRACT
```

**Rastrear origem dos créditos de uma empresa:**

O `audit.py` filtra eventos `Transfer` (inclui `mint`) e `DroneRequested` mostrando o histórico completo de cada conta — sem permissões especiais, apenas HTTP RPC público.

**Consulta direta via JSON-RPC (sem audit.py):**

```bash
# Saldo de uma conta específica (substitua ADDR e CONTRACT)
curl -s http://localhost:18541 -H 'Content-Type: application/json' -d '{
  "jsonrpc":"2.0","method":"eth_call","id":1,
  "params":[{"to":"CONTRACT","data":"0x27e235e3000000000000000000000000ADDR"},"latest"]
}'
```

---

## Estrutura do Repositório

```
P03_block_chain/
├── blockchain/
│   ├── DroneToken.sol       # Contrato Solidity — tokens + log imutável
│   ├── deploy.py            # Implanta o contrato na rede geth
│   ├── audit.py             # Auditoria pública: saldos e histórico de eventos
│   ├── Dockerfile           # Imagem do deployer
│   └── geth/
│       ├── genesis.json     # Bloco gênese — sealers + alocação inicial de ETH
│       ├── entrypoint.sh    # Inicializa datadir, importa keystore, sobe geth
│       ├── Dockerfile       # Imagem geth v1.13.15 (última versão com Clique)
│       └── genkeys.py       # Gera contas, atualiza genesis.json, grava .env
├── sector_manager/
│   ├── sector_manager.py    # Gerenciador + RA + Lamport + BlockchainLogger
│   └── Dockerfile
├── drone/
│   ├── drone_agent.py
│   └── Dockerfile
├── client/
│   ├── client.py            # Terminal empresa: paga tokens + envia MQTT
│   └── Dockerfile
├── monitor/
│   ├── monitor.py           # TUI curses
│   └── Dockerfile
├── broker/
│   ├── iot_broker.py        # MQTT do zero (TCP + UDP)
│   └── Dockerfile
├── docker-compose.yml       # Orquestração completa (~20 serviços)
└── .env                     # Chaves privadas (gerado por genkeys.py — não commitar)
```

---

## Equipe

- Yasmin Cordeiro Meira

---

## Referências

> - [1] Python Software Foundation. "socket — Low-level networking interface." Python 3 documentation. https://docs.python.org/3/library/socket.html
> - [2] Python Software Foundation. "threading — Thread-based parallelism." Python 3 documentation. https://docs.python.org/3/library/threading.html
> - [3] Python Software Foundation. "curses — Terminal handling for character-cell displays." Python 3 documentation. https://docs.python.org/3/library/curses.html
> - [4] OASIS Standard. "MQTT Version 3.1.1." OASIS, 2014. https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html
> - [5] Ricart, G.; Agrawala, A. K. "An optimal algorithm for mutual exclusion in computer networks." *Communications of the ACM*, v. 24, n. 1, p. 9–17, 1981.
> - [6] Lamport, L. "Time, clocks, and the ordering of events in a distributed system." *Communications of the ACM*, v. 21, n. 7, p. 558–565, 1978.
> - [7] Ethereum Foundation. "Go Ethereum." https://geth.ethereum.org/
> - [8] Ethereum Foundation. "EIP-225: Clique proof-of-authority consensus protocol." https://eips.ethereum.org/EIPS/eip-225
> - [9] Web3.py Contributors. "Web3.py — A Python library for interacting with Ethereum." https://web3py.readthedocs.io/
> - [10] Solidity Contributors. "Solidity Documentation." https://docs.soliditylang.org/
