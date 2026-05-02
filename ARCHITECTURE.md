# Fault-Tolerant Data Processing System

## Assumptions

Hashing assumes the raw JSON payload is the source of truth for idempotency. We serialize with `sort_keys=True` and compact separators to ensure identical payloads produce the same hash regardless of key order or whitespace. This means two payloads with semantically equivalent but structurally different JSON (e.g., `{"a":1}` vs `{"a": 1}`) will hash identically, but payloads with the same business data but different key names (e.g., `amount` vs `value`) will not.

## Deduplication

Content-based hashing prevents double counting by treating the hash as a unique identifier. Before processing, we check if the hash exists in the mock DB. If present, we return 409 Conflict without normalizing or updating aggregates. This guarantees that even if the same event is submitted multiple times (e.g., due to client retries), it only affects aggregates once. The hash is deterministic and collision-resistant (SHA-256), making it suitable as a primary key.

## Failure Handling

The system does NOT ensure consistency if the DB fails mid-request. The current implementation has a critical gap: we update `mock_db` and `aggregates` sequentially without transactions. If `mock_db[event_hash] = event` succeeds but `aggregates.record(event)` fails (or vice versa), the system enters an inconsistent state where the event exists but aggregates don't reflect it, or aggregates include an event that doesn't exist. A production system would need atomic writes, transactional storage, or an event-sourcing pattern with reconciliation.

## Scalability

The first bottleneck under high load is the in-memory `mock_db` dictionary. As a single-process data structure, it cannot scale horizontally—all requests must hit the same process. Hash lookups are O(1) but memory-bound; the system will crash when the event count exceeds available RAM. The second bottleneck is the global `aggregates` object, which becomes a contention point with concurrent writes. To scale, replace the dictionary with a distributed key-value store (Redis, DynamoDB) and aggregates with a streaming aggregation system (Kafka + Flink) or materialized views.
