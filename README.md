# Courier

[English](README.md) · [Türkçe](README.tr.md)

A transactional **outbox → Kafka → idempotent inbox** pipeline for .NET with PostgreSQL and any Kafka-compatible broker (tested on Redpanda). It solves the dual-write problem (a database change and its event must happen together) and the duplicate-delivery problem (the broker delivers at least once) with a small, tested library, plus a sample ledger service that proves the guarantees under failure.

```
 your tx ──► state + outbox row ──commit──► relay ──► Kafka ──► consumer ──► inbox row + handler effect ──commit
                                              ▲ at-least-once, per-key ordered │   (one transaction: the effect happens once)
```

## What is in it

| Capability | How |
|---|---|
| Atomic state + event | `Outbox.EnqueueAsync(connection, tx, message)` writes the event in *your* transaction; rollback publishes nothing |
| Per-key ordering | Outbox rows are sharded by a stable hash of the key; each shard is drained serially by exactly one relay (session advisory lock), producer is idempotent with `acks=all` |
| Multiple relays | Shards are leased with `pg_try_advisory_lock`; a relay that dies releases its shards with its connection |
| At-least-once publishing | Rows are marked published only after the broker acknowledges; a crash in between republishes |
| Exactly-once *effect* | Consumer inserts into the inbox and runs your handler in **one transaction**; a redelivered message hits the inbox key and is skipped |
| Batched consumption | Up to 100 messages per transaction, with automatic fallback to per-message processing when one fails |
| Retry, dead letter, replay | Retries with backoff, `PermanentFailureException` skips them, failures go to `<topic>.dlq` with error/origin headers, `DeadLetters.ReplayAsync` re-publishes after a fix |
| Offsets | Manual: stored after the transaction commits, committed on an interval and on rebalance |

## Design in one page

* **The outbox row is committed with the state it describes** ([ADR 1](docs/adr/en/0001-outbox-relay.md)). The relay is a separate process step: it reads pending rows of a shard in id order, produces them, flushes, then marks the acknowledged ids.
* **Ordering comes from sharding, not from luck.** Same key → same shard → drained by one relay in id order → same Kafka partition → consumed by one consumer in order. Idempotent producing keeps the order through retries; if a message fails, the messages after it in the partition fail too, so what was delivered is always a prefix.
* **The inbox makes the handler effect idempotent** ([ADR 2](docs/adr/en/0002-inbox-consumer.md)). Exactly-once delivery does not exist; exactly-once *effect* does, when the dedupe record and the effect share a transaction.
* **Failure never blocks a partition.** After the retry budget a message is moved to the DLQ topic and the offset moves on.

## The sample: a ledger with a read model

`Courier.Ledger` is a small service: accounts and transfers in PostgreSQL (source of truth) and a `view_balances` projection built purely from events. Each transfer emits two events (debit and credit), each carrying the account's new **version**; the projector applies an account's events only in version order and fails loudly on a gap. That makes the pipeline's guarantees observable: after any amount of crashing, `view_balances` must equal `ledger_accounts` exactly, including versions.

## Evidence

One Windows machine; PostgreSQL 17 and Redpanda 24.3 in Docker on the same machine; everything in one process. Relative, not absolute.

| Run | Result |
|---|---|
| 16 tests (below) | pass in CI |
| 20,000 transfers (40,500 events), 8 writers | 911 transfers/s at the source (p50 8.5 ms, p99 14.9 ms); the projection was fully caught up 0.3 s after the last write; 0 accounts differ |
| Same, backlog first: relay and 3 consumers started cold on 40,500 queued events | drained in 15.4 s (~2,600 events/s, includes consumer group join) |

Reproduce: `dotnet run -c Release --project bench/Courier.Bench -- --transfers 20000` (add `--drain true` for the backlog run).

### How it is tested (real PostgreSQL + real Redpanda)

* End-to-end delivery with the outbox id as message id; a rolled-back transaction publishes nothing.
* **Relay crash between "broker acknowledged" and "marked published":** injected three times; duplicates reach the consumer and the inbox absorbs every one (the test asserts duplicates were actually seen).
* **Ordering:** 20 keys × 60 sequential messages written concurrently, 2 relays and 2 consumers in one group; the handler records any out-of-sequence delivery: zero.
* **Consumer killed with offsets uncommitted** (real kill: no commit, no close); its replacement re-reads and the inbox keeps the effect count exact at 300.
* Transient failure retried without dead lettering; poison message dead-lettered after the retry budget without blocking later messages; permanent failure skips retries; dead letters replayed after the "bug is fixed".
* **Chaos:** 1,500 concurrent transfers among 24 accounts (some overdrafts) while relays are stopped/restarted, consumers killed/restarted 10 times and the relay randomly "crashes" before marking 4% of batches. At the end the projection equals the source for every account (balance and version), money is conserved, nothing is left unpublished, and the inbox holds exactly `2 × transfers + accounts` rows.
* Ledger rules (overdraft changes nothing and emits nothing, duplicate transfer ids move money once under 10 concurrent attempts) and the HTTP API.

### Bugs found while building it

* A "killed" consumer still committed its offsets on close, so the test never exercised redelivery. Offset commits are now manual, and `KillAsync` skips the final commit and close; the test now sees real redelivery.
* My first batched projector used an `INSERT … WHERE … ON CONFLICT` upsert whose `WHERE` removed the row the conflict clause needed; every event after the first per account was treated as a gap and dead-lettered. Found by the benchmark never finishing; replaced by `UPDATE … WHERE applied_version = v − 1` with an insert only for version 1.
* The project's original name made Windows deny writes to the build output (likely a security product matching the DLL name); it was renamed.

## Run it

```bash
docker compose up --build
KEY="Authorization: Bearer change-me-0123456789"
curl -X PUT localhost:8080/accounts/alice -H "$KEY" -H "Content-Type: application/json" -d '{"balance":500}'
curl -X PUT localhost:8080/accounts/bob   -H "$KEY" -H "Content-Type: application/json" -d '{"balance":0}'
curl -X POST localhost:8080/transfers -H "$KEY" -H "Content-Type: application/json" -d '{"id":"t1","from":"alice","to":"bob","amount":120}'
curl localhost:8080/view/bob -H "$KEY"
```

Using the library:

```csharp
await using var tx = await connection.BeginTransactionAsync();
// ... your state changes ...
await Outbox.EnqueueAsync(connection, tx, new OutboxMessage("orders.events", orderId, json));
await tx.CommitAsync();

new OutboxRelay(dataSource, new RelayOptions { BootstrapServers = bootstrap }).Start();
new InboxConsumer(dataSource, new ConsumerOptions { BootstrapServers = bootstrap, GroupId = "billing", Topics = ["orders.events"] },
    async (message, connection, tx, ct) => { /* your effect, in tx */ }).Start();
```

```bash
dotnet test --solution Courier.slnx
```

## Known limits

* **Per-key order holds only while one relay owns the shard.** If a relay stalls long enough to lose its database connection while a producer batch is still in flight, a second relay can publish the same rows and the broker can see them out of order relative to the first. The inbox keeps effects correct, but handlers that need strict order (like the sample's version check) will dead-letter the stale one. There is no fencing token on the produce path.
* **Rebalance races:** during a consumer rebalance the same partition can briefly be processed by two consumers; the inbox prevents double effects, ordering across that moment is best effort.
* **Retries block the partition** for the retry budget (about 0.7 s by default) before dead lettering; there are no non-blocking retry topics.
* **The inbox and the outbox grow forever.** There is no retention job for published outbox rows or old inbox rows (an inbox row older than the topic retention can be deleted safely, but that is not implemented).
* **A batch that contains one poison message** is retried once as a batch, then message by message; the poison handler is invoked `MaxAttempts + 1` times.
* **Polling relay,** 20 ms idle interval, no `LISTEN/NOTIFY`; latency floor is that interval plus `linger.ms`.
* **The handler and the inbox must share the same PostgreSQL database;** effects in other systems need their own idempotency.
* **Message schema/contract validation is the handler's job;** no schema registry integration.
* Single shared API key on the sample service. Broker authentication (SASL/TLS) is passed through the `Configure` callback on the relay and consumer options and is not covered by tests.

Design decisions: [docs/adr/en](docs/adr/en).
