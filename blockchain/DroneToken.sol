// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * Contrato de tokens para despacho de drones no sistema de monitoramento marítimo.
 *
 * Custo por criticidade:
 *   4 → 40 tokens   3 → 30 tokens   2 → 20 tokens   1 → 10 tokens
 *
 * Fluxo:
 *   1. Cliente chama requestDrone() → paga tokens → emite DroneRequested
 *   2. Setor ouve o evento e enfileira a ocorrência
 *   3. Setor registra na chain: despacho, reenfileiramento, falha ou retorno
 */
contract DroneToken {

    address public owner;
    mapping(address => uint256) public balances;

    // ── Eventos do cliente ────────────────────────────────────────────────────

    event DroneRequested(
        address indexed requester,
        uint8   indexed sector,
        string          occurrenceType,
        uint8           criticality,
        uint256         cost,
        string          requestId,
        uint256         ts
    );

    // ── Eventos do gerenciador de setor ───────────────────────────────────────

    event DroneDispatched(
        uint8   indexed sector,
        string          occurrenceId,
        string          droneId,
        string          requestId,
        uint256         ts
    );

    event OccurrenceRequeued(
        uint8   indexed sector,
        string          occurrenceId,
        string          reason,
        uint256         ts
    );

    event DroneFailed(
        uint8   indexed sector,
        string          occurrenceId,
        string          droneId,
        uint256         ts
    );

    event DroneRecalled(
        uint8   indexed sector,
        string          droneId,
        string          occurrenceId,
        uint256         ts
    );

    // ── Controle de acesso ────────────────────────────────────────────────────

    constructor() {
        owner = msg.sender;
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    // ── Gestão de tokens ──────────────────────────────────────────────────────

    event Transfer(address indexed from, address indexed to, uint256 amount);

    function mint(address to, uint256 amount) external onlyOwner {
        balances[to] += amount;
        emit Transfer(address(0), to, amount);
    }

    function transfer(address to, uint256 amount) external {
        require(balances[msg.sender] >= amount, "saldo insuficiente");
        balances[msg.sender] -= amount;
        balances[to]          += amount;
        emit Transfer(msg.sender, to, amount);
    }

    function costFor(uint8 criticality) public pure returns (uint256) {
        if (criticality >= 4) return 40;
        if (criticality == 3) return 30;
        if (criticality == 2) return 20;
        return 10;
    }

    // ── Ação do cliente ───────────────────────────────────────────────────────

    function requestDrone(
        uint8          sector,
        string calldata occurrenceType,
        uint8          criticality,
        string calldata requestId
    ) external {
        uint256 cost = costFor(criticality);
        require(balances[msg.sender] >= cost, "saldo insuficiente");
        balances[msg.sender] -= cost;
        emit DroneRequested(
            msg.sender, sector, occurrenceType,
            criticality, cost, requestId, block.timestamp
        );
    }

    // ── Registro pelo gerenciador de setor ────────────────────────────────────

    function recordDispatch(
        uint8          sector,
        string calldata occurrenceId,
        string calldata droneId,
        string calldata requestId
    ) external {
        emit DroneDispatched(sector, occurrenceId, droneId, requestId, block.timestamp);
    }

    function recordRequeue(
        uint8          sector,
        string calldata occurrenceId,
        string calldata reason
    ) external {
        emit OccurrenceRequeued(sector, occurrenceId, reason, block.timestamp);
    }

    function recordDroneFailed(
        uint8          sector,
        string calldata occurrenceId,
        string calldata droneId
    ) external {
        emit DroneFailed(sector, occurrenceId, droneId, block.timestamp);
    }

    function recordRecall(
        uint8          sector,
        string calldata droneId,
        string calldata occurrenceId
    ) external {
        emit DroneRecalled(sector, droneId, occurrenceId, block.timestamp);
    }
}
