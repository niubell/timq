# TiMQ Architecture

## Overview

TiMQ is a Kafka-compatible message queue built on TiKV's distributed key-value storage.

## Key Design Decisions

### 1. Storage Layer

- **RawKV API**: Used for message storage (logs)
  - Single-key atomic operations
  - Low latency for append-only writes
  - Efficient range scans for consumer reads

- **TxnKV API**: Used for metadata and offsets
  - Multi-key transactions for consistent metadata updates
  - ACID guarantees for consumer offset commits

### 2. Kafka Protocol Compatibility

Implemented APIs:
- `ApiVersions` (API key 18) - Protocol version negotiation
- `Metadata` (API key 3) - Topic and broker discovery
- `Produce` (API key 0) - Message publishing
- `Fetch` (API key 1) - Message consumption
- `ListOffsets` (API key 2) - Offset queries

### 3. Data Model

```
/timq/
  topics/
    {topic-name}/
      metadata/          - Topic configuration
      partitions/
        {partition-id}/
          messages/      - Message log
            {offset} -> {encoded-message}
          offsets/       - Consumer offsets
            {group-id} -> {committed-offset}
```

### 4. Message Format

```
┌─────────────┬──────────┬──────────┬────────────┬──────────┐
│ Timestamp   │ Key Len  │ Key      │ Value Len  │ Value    │
│ (8 bytes)   │ (4 bytes)│ (var)    │ (4 bytes)  │ (var)    │
└─────────────┴──────────┴──────────┴────────────┴──────────┘
```

### 5. Performance Considerations

- **Batch Writes**: Produce requests are batched for efficiency
- **Range Scans**: Fetch uses TiKV's Scan API for sequential reads
- **Offset Caching**: Latest offsets cached in memory
- **Compaction**: TiKV's built-in compaction handles log retention

## Future Improvements

1. **Consumer Groups**: Full consumer group coordination
2. **Replication**: Multi-region replication support
3. **Metrics**: Prometheus metrics integration
4. **ACL**: Kafka-style access control
5. **Exactly-Once**: Transactional message delivery

## References

- [Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [TiKV Go Client](https://tikv.org/docs/6.5/develop/clients/go/)
- [TiKV RawKV API](https://tikv.org/docs/6.5/develop/rawkv/)
