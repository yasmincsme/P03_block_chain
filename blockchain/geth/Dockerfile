FROM ethereum/client-go:v1.13.15

# v1.13.15 é a última linha do geth que ainda suporta Clique (PoA) standalone —
# versões mais novas exigem um cliente de consenso PoS (post-merge) e recusam
# redes legadas sem "terminalTotalDifficulty".

COPY genesis.json /genesis.json
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/bin/sh", "/entrypoint.sh"]
