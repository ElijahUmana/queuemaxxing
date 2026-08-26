# Architecture

## Goals

Queuemaxxing implements a durable concurrent HTTP job queue on one machine without delegating storage to a database or another message broker. Its core design goals are:

- Composable FIFO/LIFO, priority, and initial-delay behavior
- Deterministic selection and time-boundary semantics
- At-least-once leased delivery with fenced receipts
- Durable mutations acknowledged only after local WAL synchronization
- Recovery after application restart and incomplete final writes
- Safe concurrent producers and consumers
- A strict language-neutral HTTP boundary
- A sample application that uses only that boundary

Non-goals include distributed consensus, replication, multi-node failover, exactly-once downstream side effects, exchange routing, and retained replay of acknowledged messages.

## Components

```text
HTTP clients / workbench
          │
          ▼
   versioned HTTP API
          │ engine.Service
          ▼
 serialized queue state machine ─── clock/timer seam
          │ journal.Journal
          ▼
 queue-owned segmented WAL + snapshots
          │
          ▼
      local filesystem
```

### Model

`internal/model` contains transport-independent queue configuration, message lifecycle state, delivery receipts, mutation requests, pagination, and statistics.

A queue configuration includes:

- Name
- FIFO or LIFO ordering
- Whether priority participates in selection
- Default initial delay
- Default visibility timeout
- Maximum delivery count
- Creation time

A message carries immutable identity and ordering metadata plus mutable delivery state. Sequence and last-applied LSN connect the logical state machine to durable order.

### Engine

`internal/engine` owns the queue state machine. It exposes operations to create and inspect queues; enqueue and receive messages; acknowledge, nack, or extend leases; inspect messages and dead letters; redrive; report statistics; compact; and shut down.

The engine serializes incompatible mutations. Read-only inspection may operate concurrently against a coherent snapshot, but it must not observe a partially applied transaction.

### Journal

`internal/journal` owns persistence. The interface supports durable single and batch append, checkpoints, recovered records and snapshots, storage statistics, and close.

The concrete journal is responsible for:

- Exclusive process ownership of the data directory
- Framed, versioned, checksummed records
- Monotonic contiguous LSN assignment
- Sync-before-success append semantics
- Segment rotation
- Recovery and torn-tail handling
- Atomic snapshot publication
- Safe compaction
- Fail-stop behavior after uncertain storage failures

### Clock

`internal/clock` abstracts `Now` and timers. Production uses the system clock; deterministic tests inject a controlled clock so exact delay and lease boundaries can be exercised without sleeps.

### API and client

The API translates strict versioned HTTP requests to `engine.Service` calls and maps stable engine error codes to stable HTTP error envelopes. The typed client and browser workbench consume the same public contract.

The API layer owns input limits, JSON strictness, header extraction, request cancellation, and graceful server shutdown. It does not reimplement queue ordering or lifecycle logic.

## Selection semantics

Eligibility is evaluated before ordering:

```text
eligible(message, now) =
  message.state == READY
  and message.available_at <= now
  and no active lease
```

Among eligible messages:

```text
FIFO:          sequence ascending
LIFO:          sequence descending
Priority FIFO: priority descending, sequence ascending
Priority LIFO: priority descending, sequence descending
```

Priority is signed 32-bit; larger values are more urgent. The default is zero. Delay never wins a comparison; it only decides whether a message can enter the comparison.

The enqueue sequence is assigned exactly once in the serialized durable enqueue transaction. It is preserved across nack and lease expiry. Redrive creates a new message and therefore a new sequence.

This design avoids ambiguous requeue behavior:

- FIFO retries retain their original relative age.
- LIFO retries do not pretend to be a newly enqueued message.
- Equal-priority order remains deterministic after restart.
- Concurrent sends are ordered by their serialized durable commits.

## Lifecycle state machine

### Enqueue

An accepted enqueue creates one message ID and one sequence. Its initial state is:

- `delayed` when `available_at > now`
- `ready` when `available_at <= now`

An explicit absolute `available_at` and relative delay cannot be interpreted inconsistently; the API contract defines validation and precedence rather than silently combining them.

### Promotion

At `available_at <= now`, a delayed message becomes ready. Promotion is a logical state transition driven by server time. Long pollers must wake at the earliest relevant deadline or an earlier state-changing event.

### Receive

Receive atomically selects the highest-ranked eligible message, increments `delivery_count` and `lease_epoch`, produces an unguessable receipt, sets `lease_until`, and makes the delivery durable before returning success.

Reservation order is serialized. Concurrent handlers may return in a different order, and workers may complete in any order.

### Ack

Ack succeeds only when:

- Message ID identifies the leased message
- Receipt matches the current lease epoch/token
- `now < lease_until`

At exactly `lease_until`, the receipt is stale. A successful ack is terminal and durable. Old receipts can never acknowledge a later lease.

### Nack

Nack has the same receipt and time checks as ack. It atomically invalidates the lease and changes eligibility to `now + retry_delay`. Zero delay makes it immediately ready. It preserves identity, priority, and sequence.

If the current delivery count has reached `max_deliveries`, nack transitions to `dead` instead of ready/delayed.

### Lease expiry

Expiry uses the half-open lease interval: active while `now < lease_until`, expired otherwise. It follows the same requeue/dead-letter placement as nack. An ack serialized before expiry wins; one serialized at or after expiry fails stale.

### Extend

Extend requires the current unexpired receipt. It replaces the deadline with `now + requested_visibility_timeout`, creates a new durable deadline, and cannot revive an expired lease.

### Dead letter and redrive

Exactly N reservations are permitted for `max_deliveries = N`. Failure of delivery N produces a retained dead letter.

Redrive is not mutation of the source into a ready message. It creates a child message with:

- New message ID and sequence
- Delivery count zero
- Preserved payload
- Preserved priority unless overridden
- Immediate availability unless a new delay/time is supplied
- `replay_of` pointing to the source

The dead source remains auditable. The redrive transaction and its idempotent result prevent response-loss duplicates.

## Receipts and ABA prevention

A receipt is a fenced capability, not just a message ID. It binds an operation to one lease epoch. Every reservation increments the epoch and returns a fresh unguessable token.

This prevents the ABA failure in which:

1. Worker A receives a message.
2. A’s lease expires.
3. Worker B receives the same message.
4. A’s late ack incorrectly deletes B’s active delivery.

Step 4 fails because A’s receipt refers to an older epoch. Receipt values must not be exposed in list APIs, persisted in logs, or rendered as ordinary message metadata.

## Idempotency

Transport retries can happen after the server commits but before the client observes the response. Mutating operations therefore accept an idempotency key.

For each supported operation, durable idempotency state binds:

- Operation scope
- Idempotency key
- Canonical request fingerprint
- Committed result or stable terminal outcome

An identical retry returns the prior result without another mutation. Reusing a key with a different fingerprint returns `idempotency_conflict`. Idempotency entries must survive restart for their documented retention window.

Idempotency does not provide exactly-once downstream processing. It suppresses duplicate queue mutations caused by request replay.

## Mutation and durability protocol

Each logical operation follows one transaction protocol:

```text
request
  -> validate against current state
  -> build deterministic transaction/events
  -> append framed WAL transaction
  -> fsync WAL
  -> apply transaction to memory
  -> publish wakeups/result
  -> return success
```

A successful response means the WAL transaction crossed the synchronization boundary. Cancellation before commit produces no acknowledged mutation. Cancellation after durable commit cannot roll the mutation back; an idempotent retry retrieves its result.

The in-memory apply step must be deterministic and infallible for a previously validated transaction. If this invariant is violated, the process should fail rather than serve memory state that disagrees with the WAL.

## WAL framing

The concrete binary layout is owned by `internal/journal`, but every frame needs enough metadata to validate recovery without executing payloads speculatively:

- Magic and format version
- Record/frame type
- Header length and payload length
- LSN and transaction ID
- Payload bytes
- Checksum over the defined header/payload region

Lengths are bounded before allocation. Unknown required versions/types fail startup. LSNs must be contiguous across segments.

A batch that represents one logical mutation must recover atomically. Recovery must never apply half of a multi-record transaction.

## Recovery

Startup recovery proceeds in this order:

1. Acquire exclusive data-directory ownership.
2. Validate the manifest and segment ordering.
3. Load the newest valid atomically published snapshot, if present.
4. Scan later WAL segments in LSN order.
5. Validate framing, bounds, checksums, transaction completeness, and LSN continuity.
6. Truncate only an incomplete final frame in the newest writable segment.
7. Replay complete transactions deterministically into indexes.
8. Reconcile time-derived delayed and expired-lease states using the current server clock.
9. Open a writable segment and report readiness.

Recovery refuses:

- Complete-frame checksum mismatch
- Corruption before the newest tail
- Segment or LSN gaps
- Duplicate/out-of-order LSNs
- Invalid transaction structure
- Unsupported format versions
- A second live process owning the data directory

A corrupt log must produce a diagnostic with the segment, offset, LSN when known, and reason. Silent skipping would break the durability contract.

## Snapshots and compaction

A snapshot is a checksummed serialization of the complete logical state through a durable LSN. Publication follows write-temporary, sync-file, atomic-rename, and sync-directory semantics where supported.

Compaction must preserve a recovery fallback until the new snapshot and manifest are durable. Old WAL segments become deletable only after publication. A crash at any phase must recover either the old complete chain or the new complete snapshot-plus-tail chain.

Compaction can pause or increase write latency in this single-node implementation; its exact concurrency behavior must be measured and documented rather than hidden.

## Filesystem ownership and locking

Only one process may write a data directory. The lock must be held for the service lifetime and released on orderly close. Failure to acquire it is a startup error, not a cue to open concurrently.

The design assumes a local filesystem with the atomic rename and synchronization behavior required by the concrete journal. Network filesystems and external mutation of data files are outside the supported durability boundary unless explicitly tested.

## Read-only fail-stop mode

After a write, flush, sync, rotation, or snapshot-publication failure that leaves durability uncertain, the journal records a read-only reason and rejects future mutations with `storage_unavailable`. Liveness may remain true while readiness becomes false.

The service does not swallow disk-full or permission errors, claim that unsynchronized data is durable, or continue appending after a partial failure without a proven recovery transition.

## Resource bounds

Every externally controlled dimension needs an explicit bound:

- HTTP body and JSON depth/shape
- Message payload size
- Queue count and message count/bytes
- Delay, visibility, and wait durations
- Pagination limit and cursor size
- Idempotency key and stored outcome retention
- In-flight deliveries and long pollers
- WAL frame and transaction sizes
- Segment and snapshot sizes
- Failure-reason and metadata lengths

At a hard capacity limit, producers receive `capacity_exceeded`; the service must not evict acknowledgedly accepted work without an explicit retention contract.

## HTTP trust boundary

The network API is the system boundary. It must:

- Bind to loopback by default
- Require the expected content type for JSON bodies
- Reject unknown fields, duplicate keys, trailing values, invalid UTF-8 where applicable, and out-of-range numbers
- Limit request bodies before decoding
- Apply read-header, read, write, idle, and graceful-shutdown timeouts
- Propagate cancellation to long polls
- Return stable machine-readable error codes without internal paths or receipt values
- Treat cursor, receipt, and idempotency tokens as opaque

Queue names and identifiers must be validated before they affect filesystem paths. User payloads are never interpolated into HTML or shell commands.

## Observability

Service statistics expose queue/message counts and storage state, including durable LSN, WAL bytes, segment/snapshot information where implemented, last sync time, and read-only status/reason.

Readiness means the service can safely perform its configured role, including owning the data directory and having usable storage. Liveness only means the process and HTTP loop are responsive. A fail-stop storage error should therefore fail readiness without pretending the process is dead.

Metrics and logs must avoid unbounded labels from queue/message IDs and must redact receipts and other capabilities.

## Pub/Sub migration

A destructive queue stores one delivery state per message. Durable Pub/Sub needs one immutable event plus independent delivery state per subscription.

A future design would introduce:

```text
topic log: event(topic, sequence, payload, headers, published_at)
subscription: name, topic, start policy, filter, retention participation
subscription state: cursor/acks, leases, attempts, DLQ
```

Workers sharing a subscription compete for work; separate subscriptions fan out. Storage reclamation is gated by retention and participating cursor progress. Slow-subscription quotas and eviction policy are required to prevent an abandoned cursor from filling disk.

This model enables retained replay by sequence or timestamp. It is intentionally separate from dead-letter redrive.

## Failure model and limitations

The architecture addresses concurrent requests, worker crashes, application restarts, stale receipts, incomplete newest-tail writes, and detected storage corruption.

It does not address:

- Loss of the host or storage device
- Byzantine filesystems or undetected hardware corruption
- Multi-process writers
- Distributed consensus or split brain
- Exactly-once external side effects
- Global completion ordering
- Retained replay after acknowledgement
- Authentication or transport encryption unless provided by the finalized server or an external proxy

These limits define where SQS, RabbitMQ, Pulsar, NATS JetStream, or an application-embedded library is the stronger choice.

## Verification gates

Architecture claims become release claims only when repository tests reproduce them. Required gates include:

- Reference-model ordering across all feature compositions
- Deterministic boundary tests for delay and lease deadlines
- Concurrent mutation stress under Go’s race detector
- Receipt ABA and incompatible-transition races
- Restart recovery after every mutation type
- Subprocess termination around WAL write/sync/apply boundaries
- Torn-tail repair and corruption refusal
- Exclusive data-directory locking
- Injected disk-full/write/sync/rename failures
- Snapshot and compaction crash matrices
- Strict HTTP parsing and bounded-resource tests
- Real workbench browser flows through the public API

The canonical commands are maintained in the root README and must match the actual scripts and test packages present in the repository.
