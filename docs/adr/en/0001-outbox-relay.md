# ADR 0001 — Transactional outbox with a sharded, serial relay

**Status:** Accepted

## Context

Writing to the database and publishing to Kafka cannot be one atomic operation. Doing them one after the other loses events (crash between) or publishes events for state that was rolled back.

## Decision

* The event is written to a `courier_outbox` table **inside the caller's transaction**. Commit publishes it logically, rollback discards it.
* A relay reads pending rows, produces them, and marks them published only after the broker acknowledged. Publishing is therefore **at-least-once**: a crash after the acknowledgement republishes the batch.
* **Ordering:** every row has `shard = FNV-1a(key) % 16` (a stable hash; `string.GetHashCode` is randomized per process and would break ordering across restarts). A shard is drained by exactly one relay at a time, in id order. The relay holds a session-level advisory lock per shard on a dedicated connection; if the connection or process dies, PostgreSQL releases the lock and another relay takes over. A relay may cap how many shards it takes, so a second instance can share the load.
* The producer is idempotent with `acks=all`, which preserves order across internal retries. When a message of a batch fails, later messages of the same partition fail too, so the delivered set is a prefix and only acknowledged ids are marked.
* Rows carry a stable `message_id` (a GUID assigned at enqueue time) sent as a header; it is the identity used for deduplication downstream.
* Rows with `published_at` set are kept (audit, debugging); the pending index is partial so it stays small.
* A test hook, `AfterProduceBeforeMark`, lets tests crash the relay in the one window that matters.

## Consequences

* Duplicates are expected and must be absorbed by the consumer (ADR 2).
* A shard's throughput is one serial reader; parallelism comes from 16 shards.
* The order guarantee has a documented gap if two relays ever publish the same shard concurrently (README, known limits).
