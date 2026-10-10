# ADR 0003 — The ledger sample: proving the pipeline with versions

**Status:** Accepted

## Decision

* The source of truth is `ledger_accounts` (balance and a **version** incremented on every change). A transfer is a single transaction: record the transfer id (primary key makes retries idempotent), lock both accounts in id order (no deadlocks), check funds, update both, and enqueue two events, one per account, keyed by the account and carrying the new version.
* One event per account instead of one per transfer keeps the Kafka key equal to the entity whose order matters.
* The projector applies an event only if `applied_version = event.version − 1` (a single `UPDATE … WHERE`); version 1 inserts the row. An older version is a duplicate by state and is ignored; a gap is an error (retried, then dead-lettered) rather than silently skipped.
* This turns every pipeline guarantee into something a test can assert: after crashes the projection must match the source in **balance and version**, the inbox must hold exactly `2 × transfers + accounts` rows, and money must be conserved.
* Account opening is an event too (`version 1`), so the projection can be rebuilt from the topic alone.

## Consequences

* A single event stream per account means one slow account cannot reorder another.
* Rebuilding the projection requires replaying the topic with a new consumer group (the inbox is per group).
