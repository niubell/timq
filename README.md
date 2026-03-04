# TiMQ

[![CI](https://github.com/niubell/timq/actions/workflows/ci.yml/badge.svg)](https://github.com/niubell/timq/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/niubell/timq)](https://goreportcard.com/report/github.com/niubell/timq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

TiKV-based message queue with Kafka protocol compatibility.

## Features

- **Kafka Protocol Compatible**: Drop-in replacement for Kafka clients
- **TiKV Storage**: Leverages TiKV's distributed transactional key-value store
- **High Performance**: RawKV API for low-latency message operations
- **Scalable**: Horizontally scalable with TiKV's distributed architecture

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Kafka Clients  │────▶│  TiMQ Broker    │────▶│   TiKV Cluster  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                        ┌──────┴──────┐
                        ▼             ▼
                   ┌─────────┐   ┌─────────┐
                   │ RawKV   │   │ TxnKV   │
                   │ (Logs)  │   │(Offsets)│
                   └─────────┘   └─────────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design.

## Quick Start

### Prerequisites

- Go 1.21+
- TiKV cluster (single node or distributed)

### Run TiKV (Docker)

```bash
docker run -d --name tikv-server \
  -p 20160:20160 \
  -p 2379:2379 \
  pingcap/tikv:latest \
  --addr 0.0.0.0:20160 \
  --advertise-addr 127.0.0.1:20160 \
  --pd-endpoints 127.0.0.1:2379
```

### Build and Run

```bash
# Download dependencies
go mod download

# Build
make build

# Run server
./bin/timq-server --pd-endpoints=127.0.0.1:2379
```

### Create Topic

```bash
# Using Kafka console tools
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic test --partitions 1 --replication-factor 1
```

### Produce and Consume

```bash
# Produce
kafka-console-producer.sh --broker-list localhost:9092 --topic test

# Consume
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

## Development

```bash
# Run tests
make test

# Run with development mode
make dev

# Format code
make fmt
```

## Roadmap

- [x] Kafka protocol basics (Produce, Fetch, Metadata)
- [ ] Consumer groups support
- [ ] Replication
- [ ] Metrics (Prometheus)
- [ ] ACL and security
- [ ] Exactly-once semantics

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[MIT](LICENSE)

## Acknowledgments

- [TiKV](https://tikv.org/) - Distributed transactional key-value database
- [Apache Kafka](https://kafka.apache.org/) - Distributed event streaming platform
