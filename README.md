# Queuemaxxing

Queuemaxxing is a self-contained HTTP job queue for a single machine. A queue can combine FIFO or LIFO ordering, signed priorities, and initial delivery delays. Queue state is stored in a queue-owned write-ahead log rather than delegated to a database or another broker.

This repository also includes a browser workbench that uses the same public HTTP API as any other client.

## Status

The implementation is under active construction. The semantics in this document are the intended contract; the verification commands below are the release gates used to determine which claims are implemented and proven.

## Semantics at a glance

Each queue has immutable ordering configuration:

- `fifo`: older eligible messages are selected first.
- `lifo`: newer eligible messages are selected first.
- Priority disabled: sequence alone determines selection.
- Priority enabled: larger signed 32-bit priorities are selected first; sequence breaks ties according to FIFO or LIFO.
- Delay: a message is ineligible until `available_at <= server_now`. Delay is an eligibility gate, not an ordering key.

This produces four ordering policies, each of which accepts immediate or delayed messages:

| Queue configuration | Selection among eligible messages |
| --- | --- |
| FIFO | `sequence ASC` |
| LIFO | `sequence DESC` |
| Priority FIFO | `priority DESC, sequence ASC` |
| Priority LIFO | `priority DESC, sequence DESC` |

The durable enqueue sequence is unique and monotonic. Concurrent consumers are promised reservation order, not response-completion, processing-completion, or acknowledgement order.

## Message lifecycle

```text
enqueue
  ├─ available_at > now ─> delayed ── time reached ─> ready
  └─ available_at <= now ───────────────────────────> ready

ready ── receive ─> leased
  ├─ ack before lease deadline ─> acknowledged
  ├─ nack before lease deadline ─> delayed or ready
  └─ lease expires ──────────────> delayed, ready, or dead-lettered

dead letter ── redrive ─> new delayed or ready message
```

A successful receive increments `delivery_count`, increments a lease epoch, and returns an opaque receipt plus a half-open lease interval. A lease is valid only while `server_now < lease_until`. At the exact deadline it is stale.

Acknowledgement, negative acknowledgement, and extension require the current unexpired receipt. A later lease permanently fences every earlier receipt. An extension replaces the deadline with `server_now + visibility_timeout`; it cannot revive an expired lease.

A nack preserves the message ID, priority, and original sequence. It sets a new eligibility time. This means a retry is not a new enqueue and does not silently change its FIFO/LIFO rank.

With `max_deliveries = N`, at most N reservations are allowed. A timely acknowledgement of delivery N succeeds. If delivery N is nacked or expires, the message becomes a dead letter and is never reserved for delivery N+1.

## Delivery guarantee and replay

The delivery contract is **at least once**, not exactly once. A worker can complete an external side effect and crash before its acknowledgement becomes durable, so consumers must make side effects idempotent.

“Replay” has distinct meanings:

1. **Redelivery after failure.** Nack or lease expiry returns the same logical message to eligibility, retaining its ID and ordering sequence.
2. **Producer request replay.** Mutating HTTP operations accept idempotency keys. Repeating an identical committed request can return its original result; reusing a key for a different request is a conflict.
3. **Storage replay.** On startup, the service reconstructs queue state by loading a snapshot and replaying later durable WAL transactions.
4. **Dead-letter redrive.** Redrive creates a new message with a new ID and sequence, resets delivery count, preserves the payload and priority by default, and records `replay_of` with the source ID. The source dead letter remains auditable. Redrive is idempotent so a lost response cannot create multiple children.

Acknowledged messages are terminal. Arbitrary replay of acknowledged history is deliberately not claimed by this destructive queue model.

## Durability boundary

The intended write path is:

1. Validate the complete mutation without changing visible state.
2. Serialize one logical transaction into the queue-owned WAL.
3. Write the framed transaction and synchronize it to stable storage.
4. Apply it to in-memory indexes.
5. Publish the result and HTTP success.

The linearization point for a successful mutation is its durable WAL commit. The service must not acknowledge a mutation before its WAL record is synchronized. After a storage write or sync failure, the service enters a fail-stop read-only state rather than claiming uncertain durability.

Recovery may truncate only an incomplete final frame at the tail of the newest writable segment. Checksum failure in a complete frame, corruption in an older segment, LSN gaps, or invalid transaction structure must stop startup with an actionable error; the service must not guess past corruption.

Snapshots and compaction are optimizations, not alternate sources of truth. A snapshot is published atomically only after its contents are synchronized. Recovery loads the newest valid published snapshot and replays later contiguous WAL records.

The storage contract protects committed data from application-process restarts and recoverable torn tail writes on one filesystem. It does **not** provide replication, host-loss survival, multi-node failover, or cross-region durability.

## Concurrency model

HTTP requests may execute concurrently, while state-changing queue operations are serialized through one deterministic state machine. This provides a total order for queue mutations without requiring a database transaction engine.

Core invariants:

- A message belongs to exactly one lifecycle state.
- At most one active lease exists for a message.
- One receipt can commit at most one terminal or requeue transition.
- A stale receipt cannot affect a newer lease.
- A successful HTTP mutation corresponds to a durable transaction.
- Every accepted enqueue has one durable sequence and one message ID.
- Repeating a committed idempotent operation cannot create a second effect.

Long-poll receivers re-evaluate eligibility after state changes, lease expiry, delay expiry, cancellation, or timeout. The server clock is authoritative for all API time boundaries.

## Quickstart

Prerequisites:

- Go version declared in `go.mod`
- A writable local data directory

Build and test the repository:

```bash
make verify
make build
```

Start both services with persistent local storage:

```bash
docker compose up --build
```

The queue API is published at `http://127.0.0.1:8080`; the workbench is published at `http://127.0.0.1:8081`. Compose persists queue files in the `queue-data` volume and binds both ports to loopback.

For native development, inspect each finalized command-line interface before startup:

```bash
./bin/qmax -help
./bin/qmax-workbench -help
```

The workbench accepts `-listen` (default `127.0.0.1:8081`) and `-api-url` (default `http://127.0.0.1:8080`), with `QMAX_WORKBENCH_LISTEN` and `QMAX_API_URL` equivalents. The server should remain bound to loopback unless authentication and TLS are placed in front of it.

## HTTP API

The canonical machine-readable contract lives with the API implementation. The service interface defines these resources and operations:

- Create, list, and inspect queues
- Enqueue a message
- Receive one eligible message with optional long polling
- Acknowledge, nack, or extend a leased delivery
- List live messages and dead letters with cursor pagination
- Redrive a dead letter
- Read service statistics and liveness/readiness
- Trigger compaction

All client-visible times are server-authoritative timestamps. Sequence and LSN values serialize as JSON strings to avoid precision loss in JavaScript clients. Idempotency keys are transported as headers rather than message payload fields. Receipts are opaque capabilities and must not be logged or exposed as message metadata.

Stable service error categories are:

- `invalid_request`
- `not_found`
- `conflict`
- `stale_receipt`
- `idempotency_conflict`
- `capacity_exceeded`
- `storage_unavailable`
- `closed`

Request-size limits, strict JSON decoding, duplicate-key rejection, timeouts, and exact paths/status codes are defined by the API package and its contract tests; this README does not duplicate unstable route details.

## Sample application: queue workbench

The workbench is a separate HTTP client of the queue service. It must not call engine or storage packages directly. Its purpose is to make the queue’s behavior inspectable, including:

- Create FIFO, LIFO, priority FIFO, and priority LIFO queues
- Enqueue immediate and delayed messages
- Compare equal-priority tie breaking
- Run concurrent workers through receive, ack, nack, and lease extension
- Demonstrate idempotent request replay
- Allow a lease to expire and observe redelivery
- Exhaust delivery attempts, inspect the dead letter, and redrive it
- Display server-reported ready, delayed, in-flight, acknowledged, and dead counts
- Restart the queue process and observe recovered state

The browser renders payloads as data and never injects message content as HTML. Server responses remain authoritative; the UI must suppress stale asynchronous responses rather than inventing local queue order.

## From queue to Pub/Sub

Pub/Sub requires separating immutable payload storage from delivery state:

- Append each publication once to a per-topic sequence log.
- Give every durable subscription an independent cursor, acknowledgement set, lease state, retry policy, and dead-letter state.
- Multiple workers sharing one subscription remain competing consumers.
- Multiple subscriptions provide fan-out; acknowledging one never removes another subscription’s delivery.
- Support explicit start positions such as earliest retained, latest, sequence, or timestamp.
- Define ordering per topic or partition rather than claiming one global order.
- Reclaim a log segment only after retention permits it and every retention-participating subscription has advanced beyond it.
- Bound slow-subscriber backlog with quotas, backpressure, or an explicit eviction policy.

This is a storage-model migration to a retained log with cursors, not a loop that copies each enqueue into N destructive queues.

## Future features

The highest-value additions after the core contract is verified are:

1. Filtered, resumable, rate-limited bulk dead-letter redrive.
2. Exponential retry backoff with jitter and persisted attempt history.
3. Queue, payload, disk, and in-flight quotas with explicit reject/block behavior.
4. Atomic bounded batch enqueue, receive, and acknowledgement.
5. Online backup/restore, integrity inspection, and versioned storage migrations.
6. Authentication, authorization, and TLS for non-loopback exposure.
7. Retained topic/subscription mode for historical replay and fan-out.
8. Replication only after consensus, membership, fencing, and split-brain behavior are fully specified and tested.

## Why this queue instead of an incumbent?

Choose Queuemaxxing when the deployment unit is one machine and the problem is durable background work with leases, scheduling, retries, dead letters, and inspection—not a general messaging fabric. It is designed to run as one service with one local data directory, without AWS, a separate database, or a broker cluster. That makes it suitable for offline, edge, on-premises, desktop-tool, CI-runner, and small-service environments where filesystem control and a narrow operational surface matter.

Do not choose it for multi-node failover, cross-region durability, elastic managed throughput, broad messaging protocols, exchange routing, multi-tenant retained streams, or geo-replication.

| System | Prefer it when |
| --- | --- |
| Amazon SQS | You want AWS-managed availability, redundant storage, and elastic service scale. |
| RabbitMQ | You need mature AMQP protocols, exchange routing, plugins, or replicated broker queues and streams. |
| Apache Pulsar | You need partitioned, multi-tenant retained streams, independent subscriptions, tiered storage, or geo-replication. |
| NATS JetStream | You want a lightweight single-server messaging platform with subjects, retained streams, consumers, replay, and a clustering path. |
| Embedded queue library | You need in-process calls or atomic composition with the application’s own database transaction. |
| Queuemaxxing | You need a language-neutral, inspectable, single-node job queue without another data service. |

Queuemaxxing does not claim exactly-once processing, high availability, unique single-binary deployment, or superior performance without reproducible evidence.

## Limitations and trust boundaries

- Single process and single writable data directory
- No replication or automatic failover
- At-least-once delivery; consumers own downstream idempotency
- No replay of acknowledged history
- Queue ordering governs reservation, not completion order
- Local wall-clock time controls delay and lease deadlines
- Compaction may temporarily trade write latency for reclaimed disk space
- Filesystem and hardware durability remain below the process-level WAL contract
- Loopback is the safe default; production network exposure requires external security controls until native auth/TLS exist
- Receipts and idempotency keys are security-sensitive capabilities

## Reproducible evidence

Run the repository’s canonical verification gate:

```bash
make verify
make build
```

`make verify` performs formatting checks, `go vet`, a shuffled coverage run, and the race detector. The coverage profile is written to `artifacts/coverage.out`. Run integration tests explicitly when the integration package is present:

```bash
make test-integration
```

Run tests repeatedly to expose order- and timing-sensitive flakes:

```bash
go test -count=20 ./...
go test -race -count=10 ./...
```

List benchmarks and execute them without inventing performance claims:

```bash
go test -run '^$' -bench . -benchmem ./...
```

Release evidence must additionally cover these named mechanisms through repository tests or scripts:

- All eight FIFO/LIFO × priority on/off × immediate/delayed combinations
- Priority and sequence tie-breaking
- Concurrent producer/consumer stress under the race detector
- Lease-boundary ack/nack/extend races and stale receipt fencing
- Idempotency response-loss replay and key/body conflict
- Maximum-delivery dead-letter transition and idempotent redrive
- Graceful restart and abrupt process termination after committed writes
- Torn newest-tail recovery and refusal of mid-log corruption
- Single-process data-directory locking
- Disk write/sync failure entering fail-stop read-only mode
- Snapshot publication, recovery, and compaction crash points
- Strict/malformed/oversized HTTP input and stable error envelopes
- Browser workbench golden path and hostile payload rendering

A green command is evidence only for the code revision that produced it. Performance and durability claims must cite the exact command, platform, configuration, and output.

## Design documentation

See [`docs/architecture.md`](docs/architecture.md) for component boundaries, state-machine rules, persistence flow, recovery, and tradeoffs.

## References

- [Amazon SQS delay queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html)
- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [RabbitMQ quorum queues](https://www.rabbitmq.com/docs/quorum-queues)
- [RabbitMQ streams](https://www.rabbitmq.com/docs/streams)
- [Apache Pulsar architecture](https://pulsar.apache.org/docs/4.1.x/concepts-architecture-overview/)
- [NATS JetStream retention policies](https://docs.nats.io/learn/jetstream/retention-policies)
