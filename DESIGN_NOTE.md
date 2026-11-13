# FDE L2 Design Note: Idempotent Background Transaction Processing Service

## 1. Architecture Overview

The service follows a clean **Ports and Adapters (Hexagonal) Architecture** in **Java 21**, strictly separating ingestion adapters, domain validation, idempotency/ordering state coordination, and observability concerns (`L2-FR-01`, `L2-FR-10`):

| Layer | Package & Key Classes | Responsibility |
| :--- | :--- | :--- |
| **Ingestion Adapter Layer** | `adapter.TransactionInputSource`, `InMemoryTransactionInputSource`, `JsonFileTransactionInputSource` | Decouples transaction ingestion from core business processing. Can be swapped for a Kafka consumer, AWS SQS poller, or JDBC outbox reader without modifying core domain logic. |
| **Domain & Lifecycle Model** | `domain.TransactionRequest`, `TransactionStateRecord`, `AccountState`, `TransactionStatus`, `StatusTransition` | Immutable Java 21 `record` payloads and thread-safe state records tracking every transition (`RECEIVED`, `PENDING`, `PROCESSING`, `RETRY_PENDING`, `PROCESSED`, `DUPLICATE`, `FAILED`). |
| **Validation & Resilience Layer** | `service.TransactionValidator`, `FailureSimulator`, `TransactionProcessingService` | Isolates malformed records prior to state mutation, enforces Option A sequence ordering, and executes bounded retries on transient faults. |
| **Idempotency & Ledger Store** | `repository.IdempotencyAndStateStore`, `InMemoryIdempotencyAndStateStore` | Enforces atomic commit boundaries across balance updates, processed-transaction registration, and lifecycle state transitions. |
| **Observability & Audit Layer** | `observability.StructuredAuditLogger`, `ProcessingReporter` | Emits structured JSON logs with PII masking (`***201`) and generates deterministic batch processing summaries (`L2-FR-11`). |

## 2. Idempotency Design

Enterprise financial pipelines experience at-least-once delivery from upstream queues, API gateways, and retrying clients. To guarantee **exactly-once business side effects** (`Section 6.1`, `L2-FR-05`):

1. **Separated Identity Keys:** Each request carries both a transport-level `requestId` (`idempotencyKey`) and a domain-level `transactionId`.
2. **Atomic Commit Boundary (`applyTransactionAtomically`):** Updating the account balance (`AccountState.apply`), recording `transactionId` in `processedTransactionIds`, removing it from `bufferedTransactionIds`, and transitioning the record status to `PROCESSED` occur inside a single synchronized critical section (simulating a `SERIALIZABLE` / `READ COMMITTED` database transaction with `UNIQUE (transaction_id)` constraints).
3. **Zero Partial Updates (`Section 6.4`):** Transient exceptions (`TransientProcessingException`) or business rule violations (such as insufficient funds on a debit) throw prior to committing state mutations, ensuring a failed attempt never leaves a half-applied balance or a falsely marked idempotency key.

## 3. Duplicate Detection Logic

The service implements **two-tier, lifecycle-aware duplicate detection** (`L2-FR-06`):

* **Tier 1 — Transport Replay Detection (`requestId`):** If the exact same `requestId` arrives more than once, it is immediately flagged as `DUPLICATE`.
* **Tier 2 — Business Re-submission Detection (`transactionId`):** Clients often generate a new `requestId` when retrying a timed-out call for the same business transaction (for example, Section 9.8 Row 3: `transactionId = TXN-2002` arriving with `requestId = REQ-2-DUP` after `REQ-2`).
* **Buffered + Processed Awareness:** `IdempotencyAndStateStore.isTransactionBufferedOrProcessed(transactionId)` checks both `processedTransactionIds` **and** `bufferedTransactionIds`. Whether the first request has already settled (`PROCESSED`) or is currently waiting in the out-of-order sequence buffer (`PENDING`), any subsequent duplicate is deterministically caught, transitioned `RECEIVED -> DUPLICATE`, and logged without mutating account balances.

## 4. Out-of-Order Handling Strategy (Option A: Sequence-Based Processing)

We selected **Option A: Sequence-Based Processing** (`Section 9.5`) because financial ledgers containing `CREDIT`, `DEBIT`, and `REVERSAL` operations are order-dependent: executing a `DEBIT` (`TXN-2002`, `seq=2`, `amount=40`) before its funding `CREDIT` (`TXN-2001`, `seq=1`, `amount=100`) would either cause a false overdraft failure or produce an invalid intermediate ledger balance.

### How It Works
* **Monotonic Cursor & Min-Heap Buffer:** The processor maintains an `expectedSequence` cursor (initialized to `1`) and a min-heap `PriorityQueue<BufferedEntry>` ordered by `sequenceNumber`.
* **Future Sequence Arrival (`sequence > expectedSequence`):** When `TXN-2002` (`seq=2`) arrives first, it transitions `RECEIVED -> PENDING`, registers in `bufferedTransactionIds`, and waits in the priority queue.
* **In-Order Arrival & Automatic Cascading Drain (`sequence == expectedSequence`):** When `TXN-2001` (`seq=1`) arrives, it executes immediately (`CREDIT 100` on `ACC-201`), increments `expectedSequence` to `2`, and invokes `drainReadyBuffer()`. The buffer immediately pops `TXN-2002` (`seq=2`), transitions it `PENDING -> PROCESSING -> PROCESSED` (`DEBIT 40`, settling `ACC-201` at `60.00`), and advances `expectedSequence` to `3`—all before Row 3 (`REQ-2-DUP`) is evaluated.
* **Handling the Section 9.8 Nuance (`OrderingScope`):** In Section 9.8's sample data, sequence numbers `1..5` are assigned across the batch (`ACC-201` has `1, 2, 3`; `ACC-202` has `4`; `ACC-203` has `5`). In real production systems, sequence numbers may be scoped either **per partition/stream** (`OrderingScope.STREAM_SEQUENCE`) or **per customer account** (`OrderingScope.ACCOUNT_SEQUENCE`). We implemented **both modes** via `ProcessorConfig` and verified both in unit tests. Furthermore, when a malformed record (`TXN-2004` at `seq=4` with `amount = -10`) is isolated and rejected (`FAILED`), the cursor advances past `4` to `5` so `TXN-2005` (`seq=5`) is not deadlocked by a rejected payload.

### Trade-offs
* **Pros:** Guarantees strict causal ordering and prevents false overdraft rejections on out-of-order debits/reversals.
* **Cons:** Head-of-line blocking if an upstream producer permanently drops a sequence number. In our implementation, unresolved gaps remain in `PENDING` and are explicitly surfaced in `ProcessingSummary.missingSequences()` for operator alerting or dead-letter recovery.

## 5. Retry and Failure Handling

Resilience (`Section 6.3`, `Section 9.6`, `L2-FR-08`, `L2-FR-09`) is implemented by distinguishing **non-retryable failures** from **transient failures**:

* **Non-Retryable Validation & Domain Failures:** Malformed payloads (such as `TXN-2004` with `amount = -10`) fail fast in `TransactionValidator`, transition directly `RECEIVED -> FAILED`, and are isolated so the batch continues uninterrupted.
* **Retryable Transient Failures:** When `FailureSimulator` (or a transient downstream lock/network timeout) raises `TransientProcessingException`, the service:
  1. Increments the record's attempt counter.
  2. Transitions the status from `PROCESSING -> RETRY_PENDING` and logs `TRANSIENT_FAILURE_RETRY_SCHEDULED`.
  3. Re-attempts execution up to `ProcessorConfig.maxRetries()` (default `3`). Because the failed attempt threw before `applyTransactionAtomically`, retrying never produces duplicate balance updates.
  4. If all retries are exhausted, removes the transaction from buffered state and transitions `RETRY_PENDING -> FAILED` with full diagnostic traceability.

## 6. Observability Approach

Operational visibility (`Section 6.5`, `Section 6.6`, `L2-FR-11`) is built into every stage of execution:

1. **Structured JSON Audit Logs (`StructuredAuditLogger`):** Every lifecycle event (`BUFFERED_OUT_OF_ORDER`, `PROCESSING_STARTED`, `TRANSACTION_PROCESSED`, `DUPLICATE_IGNORED`, `VALIDATION_FAILED`, `TRANSIENT_FAILURE_RETRY_SCHEDULED`, `RETRY_EXHAUSTED_FAILED`) emits a machine-parseable JSON line containing `timestamp`, `event`, `transactionId`, `requestId`, `accountRef`, `sequence`, `status`, `attempt`, and `detail`.
2. **PII / Sensitive Data Masking (`Section 6.6`):** Customer account identifiers (`ACC-201`) are automatically masked (`***201`) in all logs and console tables.
3. **Processing Summary Report (`ProcessingReporter`):** Produces a structured `ProcessingSummary` containing total ingested count, counts by final `TransactionStatus`, unique business transactions applied, total transient retry count, unresolved missing sequence numbers, per-request transition histories, and settled account balances.

## 7. AI-Assisted Development Usage & Validation

In alignment with the FDE readiness criteria (`Section 2`, `Section 7`, `Section 9.7`):

* **How AI Improved Speed:** AI assistance accelerated scaffolding the hexagonal package structure, boilerplate Java 21 `record` definitions, zero-dependency JSON file parsing, and formatting the tabular console report.
* **How Generated Logic Was Critically Validated & Refined:**
  * *Sequence Scope Ambiguity Detection:* Rather than blindly assuming sequence numbers in Section 9.8 were strictly per-account starting at `1` (which would have left `TXN-2005` on `ACC-203` stuck waiting for sequences `1..4` on `ACC-203`), we inspected the Section 9.8 test table, identified that `sequence: 2, 1, 2, 3, 4, 5` spans across accounts with `seq=4` failing validation, and engineered `OrderingScope` (`STREAM_SEQUENCE` vs. `ACCOUNT_SEQUENCE`) plus cursor advancement on rejected records.
  * *Buffered Duplicate Edge Case:* We verified that duplicate detection checks both `processedTransactionIds` and `bufferedTransactionIds` so duplicates arriving *while* the original transaction is still `PENDING` are caught just as reliably as post-settlement duplicates.
  * *Automated JUnit 5 Verification:* Every requirement (`L2-FR-01` through `L2-FR-12`) is backed by deterministic JUnit 5 assertions checking exact balance arithmetic (`BigDecimal`), status counts, transition chains, retry exhaustion, sequence gaps, and log PII redaction.

## 8. Production Hardening Recommendations

To evolve this lightweight service into a high-throughput enterprise production deployment:

1. **Durable ACID Storage & Outbox Pattern:** Replace `InMemoryIdempotencyAndStateStore` with PostgreSQL or CockroachDB. Use a composite table strategy: an `idempotency_keys` table (`PRIMARY KEY (transaction_id)`) updated in the same local database transaction as `account_balances` (`SELECT ... FOR UPDATE`) and an `outbox_events` table for downstream notifications.
2. **Partitioned Message Broker Ingestion:** Connect `TransactionInputSource` to **Apache Kafka** partitioned by `accountId`. Partitioning by `accountId` guarantees single-writer affinity per account across horizontal worker replicas, eliminating cross-node lock contention.
3. **Bounded Buffer TTL & Dead-Letter Queue (DLQ):** Persist `PENDING` out-of-order records in Redis or PostgreSQL with a configurable gap timeout (e.g., `60s`). If a missing prior sequence does not arrive within the window, route the stuck record to a Dead-Letter Queue (`DLQ`) and emit a PagerDuty/Prometheus alert (`tx_sequence_gap_stuck_total`).
4. **Exponential Backoff with Jitter & Circuit Breaking:** Replace synchronous in-loop retries with Resilience4j exponential backoff + jitter (`100ms -> 200ms -> 400ms`) or delayed retry topics, paired with a circuit breaker to protect downstream ledger databases during outages.
5. **OpenTelemetry Metrics & Distributed Tracing:** Export Micrometer counters (`transactions_processed_total{status=...}`, `transactions_retry_total`) and propagate W3C `traceparent` headers from upstream APIs/queues into structured logs.
