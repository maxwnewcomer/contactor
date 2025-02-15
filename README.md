# contactor

A distributed, eventually persisted, websocket framework designed for real-time applications.

Initially designed to be compatible with [Yjs](https://yjs.dev/), this framework allows for efficient communication between clients and servers, enabling collaborative features in applications.

## Features

- **Relay Nodes**: Manage WebSocket connections and facilitate communication between clients and rooms.
- **Redis Integration**: Utilizes Redis for storing room and node information, ensuring data persistence and quick access.
- **Scalability**: Supports multiple relay nodes, allowing for horizontal scaling of the application.
- **Real-time Updates**: Provides real-time updates to clients about room status and participant information.
- **TUI (Terminal User Interface)**: A terminal-based interface for monitoring and managing the relay nodes.
- **Yjs Compatibility**: This framework is designed to be compatible with [Yjs](https://yjs.dev/), allowing for efficient communication between clients and servers, enabling collaborative features in applications.
- **Relay Node Connection Reuse**: Relay nodes reuse existing connections to other relay nodes, allowing for efficient communication between relay nodes.
- **Room Takeover**: If a relay node goes down, another relay node will take over the room, ensuring that the room is never lost.

## System Design

> This section of the readme is still WIP

### Components

- **Relay Nodes**: These nodes manage WebSocket connections and facilitate communication between clients and rooms. They are responsible for handling incoming messages and broadcasting them to the appropriate recipients.
- **Redis or Redis Cluster**: Acts as the data store for room and node information. Redis provides fast access to data and ensures that the state of rooms and nodes is persisted across sessions.
- **DB/Storage Persistence Layer**: This layer is intended for future enhancements, allowing for persistent storage beyond Redis. It will enable the application to store historical data and provide more robust data management capabilities.

### Architecture Overview

The architecture consists of multiple relay nodes that communicate with each other and with a Redis instance. Clients connect to the relay nodes via WebSocket, allowing for real-time communication. The relay nodes handle the logic for managing rooms and broadcasting messages, while Redis serves as the backend data store for maintaining the state of the application.

## Local Development

To run two RelayNodes and a redis cluster locally:

```shell
docker compose up
```

To run the Tui just run

```shell
cargo run --bin contactor-tui
```

## Deployment

WIP

## TUI Screenshots

<img width="1316" alt="Screenshot 2024-09-15 at 11 38 32 AM" src="https://github.com/user-attachments/assets/7dcd2f44-b602-43af-bd6d-057ddeba24bb">
<img width="1324" alt="Screenshot 2024-09-15 at 11 38 39 AM" src="https://github.com/user-attachments/assets/cc40911a-93b2-4dab-84e7-0890bb6f7bf1">
