# ADR 0002 — Inbox consumer: exactly-once effect

**Status:** Accepted

## Decision

* Exactly-once *delivery* does not exist; exactly-once *effect* does when the "have I done this?" record and the effect commit together. For each message the consumer opens one PostgreSQL transaction, inserts `(consumer group, message id)` into `courier_inbox` with `ON CONFLICT DO NOTHING`, runs the handler with the same connection and transaction if the row was new, and commits. A redelivered message finds its inbox row and is skipped.
* **Batching:** up to `BatchSize` messages are consumed and handled in one transaction (one multi-row inbox insert, handlers in order, one commit). If anything in the batch throws, the transaction is rolled back (including the inbox rows) and the messages are handled one by one through the retry path. A poison message therefore costs one extra handler call, and healthy messages in its batch are not affected.
* **Retries and dead letters:** the handler is retried with exponential backoff up to `MaxAttempts`; `PermanentFailureException` skips retries. Afterwards the message is produced to `<topic>.dlq` with the original topic/partition/offset, the consumer group, the error text and the original headers, and the offset moves on, so one bad message never blocks a partition. `DeadLetters.ReplayAsync` republishes dead letters to their original topic with the original message id.
* **Offsets are manual.** They are stored after the transaction commits, committed on an interval and when partitions are revoked, and committed and closed on a graceful stop. `KillAsync` skips both, which is what a crash looks like and what the tests use.
* Messages from foreign producers without a `message-id` header are identified by `topic/partition/offset`.
* Consumers are cooperative-sticky to limit rebalance churn.

## Consequences

* The handler must write only to the same PostgreSQL database for the guarantee to hold; other side effects need their own idempotency keys (`message.MessageId` is stable).
* Inbox rows accumulate (README, known limits).
* Redelivery after a crash is normal and cheap: it ends at the inbox insert.
