# yrs-relay

A [Yjs](https://yjs.dev/) compatible, highly available, websockets server.

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
cargo run --bin yrs-relay-tui
```

## Deployment

WIP
