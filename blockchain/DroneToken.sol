// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract DroneToken {
    address public owner;
    mapping(address => uint256) public balances;

    event DroneRequested(
        address indexed requester,
        uint8   indexed sector,
        string  occurrenceType,
        uint8   criticality,
        uint256 cost,
        string  requestId,
        uint256 ts
    );

    event Minted(address indexed to, uint256 amount);

    constructor() {
        owner = msg.sender;
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    function mint(address to, uint256 amount) external onlyOwner {
        balances[to] += amount;
        emit Minted(to, amount);
    }

    function costFor(uint8 criticality) public pure returns (uint256) {
        if (criticality >= 4) return 40;
        if (criticality == 3) return 30;
        if (criticality == 2) return 20;
        return 10;
    }

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
            msg.sender,
            sector,
            occurrenceType,
            criticality,
            cost,
            requestId,
            block.timestamp
        );
    }
}
