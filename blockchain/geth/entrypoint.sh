#!/bin/sh
set -e

DATADIR=/data/node
PASSFILE=/data/pass.txt
KEYFILE=/data/sealer.key

mkdir -p /data
echo "senha123" > "$PASSFILE"

# Inicializa a chain a partir do genesis.json (idempotente — só roda na 1a vez)
if [ ! -d "$DATADIR/geth/chaindata" ]; then
    echo "[entrypoint] inicializando datadir com genesis.json"
    geth --datadir "$DATADIR" init /genesis.json
fi

# Importa a conta sealer no keystore deste nó (idempotente)
if [ -z "$(ls -A "$DATADIR/keystore" 2>/dev/null)" ]; then
    echo "[entrypoint] importando conta sealer $SEALER_ADDR"
    echo -n "$SEALER_KEY" > "$KEYFILE"
    geth account import --datadir "$DATADIR" --password "$PASSFILE" "$KEYFILE"
fi

exec geth \
    --datadir "$DATADIR" \
    --networkid "${NETWORK_ID:-1337}" \
    --http --http.addr 0.0.0.0 --http.port "${HTTP_PORT:-8545}" \
    --http.api eth,net,web3,personal,miner,clique \
    --http.corsdomain "*" \
    --port "${P2P_PORT:-30303}" \
    --nodekeyhex "$NODE_KEY" \
    --bootnodes "$PEERS" \
    --mine \
    --miner.etherbase "$SEALER_ADDR" \
    --unlock "$SEALER_ADDR" \
    --password "$PASSFILE" \
    --allow-insecure-unlock \
    --syncmode full
