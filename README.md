# contactor

A distributed, eventually persisted, websocket framework.

Initially designed to be compatible [Yjs](https://yjs.dev/).

## System Design

> This section of the read me is still WIP

### Components

- Relay Nodes
- Redis or Redis Cluster
- DB/Storage Persistence Layer

## Local Development

To run two RelayNodes and a redis cluster locally, fill out a `.env` based on `.env.template` and run

```shell
docker compose up
```

To run the Tui just run

```shell
cargo run --bin contactor-tui
```

## Deployment

WIP

## Screenshots

<img width="1316" alt="Screenshot 2024-09-15 at 11 38 32 AM" src="https://github.com/user-attachments/assets/7dcd2f44-b602-43af-bd6d-057ddeba24bb">
<img width="1324" alt="Screenshot 2024-09-15 at 11 38 39 AM" src="https://github.com/user-attachments/assets/cc40911a-93b2-4dab-84e7-0890bb6f7bf1">
