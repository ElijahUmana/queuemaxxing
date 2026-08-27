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

An enqueue request rejects simultaneous `delay_ms` and `available_at`. When `delay_ms` is omitted, the queue’s default delay still applies; an explicit `available_at` can only postpone that default eligibility time, not make the message eligible earlier.

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

Step 4 fails because A’s receipt refers to an older epoch. Receipt values are durably stored as lease state for restart recovery, but they must not appear in list APIs, application/HTTP logs, or ordinary message metadata.

## Idempotency

Transport retries can happen after the server commits but before the client observes the response. Mutating operations therefore accept an idempotency key.

For each supported operation, durable idempotency state binds:

- Operation scope
- Idempotency key
- Canonical request fingerprint
- Committed result or stable terminal outcome

An identical retry returns the prior result without another mutation. Reusing a key with a different fingerprint returns `idempotency_conflict`. Idempotency entries survive restart and expire after the configured retention period, 24 hours by default. The default cap is 1,000,000 retained idempotency records.

Idempotency does not provide exactly-once downstream processing. It suppresses duplicate queue mutations caused by request replay.

## Mutation and durability protocol

Each logical operation follows one transaction protocol:

```text
request
  -> bounded coordinator admission
  -> validate and apply speculatively in serialized request order
  -> encode one deterministic WAL record for each successful state change
  -> append the bounded record group with Journal.AppendBatch
  -> synchronize WAL and durable head once for the group
  -> stamp authoritative LSNs
  -> publish wakeups/results
  -> return success
```

The engine coordinator admits at most 256 queued mutation requests, collects at most 64 state-changing records or 8 MiB of encoded payload for one group, and waits at most 250 microseconds to collect neighbors. Invalid requests fail independently and their speculative state is restored without poisoning valid requests in the same group. A journal failure restores every successful speculative mutation in reverse order; no read can observe tentative state because the state lock remains held through the durability boundary. Receive reservations use the same coordinator. Close stops new admission, drains accepted requests, waits for their results, and then closes the journal.

Cancellation before coordinator admission or inclusion produces no mutation. Once included in a durability group, cancellation cannot withdraw the record; the operation completes according to the committed result and idempotency semantics. A successful response means the WAL transaction crossed the synchronization boundary. An idempotent retry retrieves the durable result after response loss.

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

A snapshot is a checksummed serialization of the complete logical state through a durable LSN. Publication writes a temporary file, synchronizes it, renames it within the rooted store, and synchronizes the containing directory.

Compaction must preserve a recovery fallback until the new snapshot and manifest are durable. Old WAL segments become deletable only after publication. A crash at any phase must recover either the old complete chain or the new complete snapshot-plus-tail chain.

Compaction serializes with queue mutations while it materializes retention changes, writes the snapshot, and checkpoints the journal. It can therefore pause or increase write latency; no compaction-latency bound is claimed.

## Filesystem ownership and locking

Only one process may write a data directory. The lock must be held for the service lifetime and released on orderly close. Failure to acquire it is a startup error, not a cue to open concurrently.

The crash-durability contract assumes a local filesystem that honors the file synchronization, directory synchronization, rooted rename, and exclusive locking used by the journal. It is asserted on Linux, macOS, DragonFly BSD, FreeBSD, NetBSD, and OpenBSD. Windows uses rooted rename, calls `Sync` on opened files and containing directories, propagates synchronization failures, and holds an exclusive lock, but Go does not document Windows directory-handle `Sync` strongly enough to claim equivalent power-loss durability; no journal-specific write-through rename flag is used. Other platforms fail journal startup because exclusive locking is unsupported. Network filesystems and external mutation of data files are outside the supported durability boundary. On platforms with Unix permission bits, startup normalizes managed directories and regular files to owner-only `0700` and `0600` modes through rooted opened handles.

`SegmentSize` is a rotation target, not a hard frame limit. A single atomic append request or one group-commit buffer may exceed it and remains in one segment and one synchronization boundary; the next append rotates first. This avoids empty segments and preserves request atomicity. Individual record and snapshot payloads remain bounded at 64 MiB. An atomic journal `AppendBatch` accepts at most 1,024 records and at most 64 MiB plus one record frame of total encoded bytes; over-limit requests are rejected before payload cloning or queue admission and cannot advance the durable LSN.

## Read-only fail-stop mode

After a write, flush, sync, rotation, or snapshot-publication failure that leaves durability uncertain, the journal records a read-only reason and rejects future mutations with `storage_unavailable`. Liveness may remain true while readiness becomes false.

The service does not swallow disk-full or permission errors, claim that unsynchronized data is durable, or continue appending after a partial failure without a proven recovery transition.

## Resource bounds

Default engine limits are 1,000 queues, 1,000,000 messages service-wide, 100,000 messages per queue, 1 MiB payloads, 1,000,000 idempotency records, 256-byte idempotency keys, 30-second long polls, 12-hour visibility timeouts, and 30-day delays. The HTTP layer allows at most 50 list results, bounds JSON request bodies to 1 MiB plus 64 KiB of envelope overhead, applies a 35-second request deadline, and admits at most 256 concurrent non-health requests and 64 receives with a positive wait timeout. Health probes are exempt; zero-wait receives use only the general request permit. Admission overload fails immediately with HTTP 429, `capacity_exceeded`, and `Retry-After: 1`. WAL and snapshot payload framing is bounded at 64 MiB.

At a hard queue or message capacity limit, producers receive `capacity_exceeded`; accepted work is not evicted to admit new work. Acknowledged messages and dead letters count toward the configured message limits until compaction prunes eligible retained state. Operators must therefore monitor message and WAL growth and compact deliberately; the service does not claim automatic disk quotas, producer blocking, or eviction.

## HTTP trust boundary

The queue API and workbench are network boundaries. Neither has native authentication, authorization, or TLS, and both bind to loopback by default. Both require an IP-literal listen host and reject hostnames, including `localhost`, to avoid name-resolution and bind-scope drift. Each rejects non-loopback IPs unless the operator enables that binary’s `--allow-non-loopback` flag or environment opt-in; this changes exposure validation but supplies no security controls. Non-loopback deployment requires a trusted authenticated TLS reverse proxy and network policy. Within that exposure model, the API:

- Bind to loopback by default
- Require the expected content type for JSON bodies
- Reject unknown fields, duplicate keys, trailing values, invalid UTF-8 where applicable, and out-of-range numbers
- Limit request bodies before decoding
- Apply read-header, read, write, idle, and graceful-shutdown timeouts
- Propagate cancellation to long polls
- Return stable machine-readable error codes without internal paths or receipt values
- Treat cursor, receipt, and idempotency tokens as opaque

Journal file operations are rooted beneath the operator-selected data directory. Managed subdirectories are rejected when they are symlinks, and discovered WAL and snapshot entries must be regular files. Queue names and identifiers do not become filesystem paths. User payloads are never interpolated into HTML or shell commands.

The Compose deployment explicitly opts both services into non-loopback binds only inside its container network so the workbench can reach the queue and the host can publish each container port. Host publication remains `127.0.0.1` for both services. This topology does not turn the workbench into an authorization boundary or add transport security.

The server enters draining mode before HTTP shutdown, rejects new non-health operations, permits accepted handlers to finish within the shutdown deadline, and closes the queue service only after HTTP draining completes. This ordering prevents service closure from invalidating already accepted requests.

## Pagination contract

Message and dead-letter pages are ordered by durable enqueue sequence. The cursor is an opaque versioned string transport; clients must not parse its representation or treat it as a number. The token binds the queue, normalized state filter, live/dead endpoint scope, durable snapshot LSN, snapshot generation, snapshot time, initial sequence high-water mark, and last returned sequence. A continuation resumes after that sequence only within the bound scope; cross-queue, cross-filter, and live/dead reuse is rejected. Messages enqueued or durably mutated after the initial page are excluded, and time-derived state membership is evaluated at the bound snapshot time. Retention deletion occurs only during explicit compaction; a pruning compaction advances the snapshot generation and makes older cursors unavailable rather than silently truncating their membership. Each page exposes the bound `snapshot_lsn` separately from `next_cursor`. The current token version and encoding are internal and may change without changing the opaque-string API contract.

## Observability

Service statistics expose queue/message counts and storage state, including durable LSN, WAL bytes, segment/snapshot information where implemented, last sync time, and read-only status. When read-only, public `read_only_reason` is the stable category `storage operation failed`; exact path-bearing causes remain internal.

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
- Authentication or transport encryption; non-loopback deployments require an external authenticated TLS boundary

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
- Snapshot publication, fallback recovery, and injected compaction failures
- Strict HTTP parsing and bounded-resource tests
- Real workbench browser flows through the public API

The canonical commands are maintained in the root README and match the repository’s current scripts and test packages. A configured workflow is a gate definition, not evidence of a successful run.
