# Idempotent Background Transaction Processing Service (FDE L2 Assessment)

A production-aware background transaction processing service written in **Java 21** that safely ingests asynchronous transaction requests arriving out of order or with duplicates, enforces strict idempotency, isolates malformed inputs, recovers from transient failures via bounded retries, and emits structured audit telemetry.

## Quick Links

* **Architecture & Trade-Offs (8-Part Design Note):** [`DESIGN_NOTE.md`](./DESIGN_NOTE.md)
* **Sample Input (Section 9.8 Suggested Test Data):** [`sample_data/section_9_8_input.json`](./sample_data/section_9_8_input.json)
* **Sample Expected JSON Output:** [`sample_data/section_9_8_expected_output.json`](./sample_data/section_9_8_expected_output.json)
* **Sample Console Audit Logs & Summary Report:** [`sample_data/section_9_8_console_report.txt`](./sample_data/section_9_8_console_report.txt)

## Requirements Coverage (FDE L2)

| Requirement ID | Description | Implementation |
| :--- | :--- | :--- |
| **L2-FR-01** | Configurable / replaceable input source | `TransactionInputSource` interface with `JsonFileTransactionInputSource` and `InMemoryTransactionInputSource` |
| **L2-FR-02** | Validate mandatory fields & isolate malformed records | `TransactionValidator` rejects negative/zero amounts, missing IDs, or invalid sequences (`RECEIVED -> FAILED`) without halting the batch |
| **L2-FR-03** | Maintain transaction processing state | `TransactionStateRecord` tracks current status, attempt count, and full `List<StatusTransition>` audit history |
| **L2-FR-04** | Full status lifecycle support | `TransactionStatus`: `RECEIVED`, `PENDING`, `PROCESSING`, `RETRY_PENDING`, `PROCESSED`, `DUPLICATE`, `FAILED` |
| **L2-FR-05** | Process each unique business transaction only once | `InMemoryIdempotencyAndStateStore.applyTransactionAtomically` guards balance updates via `processedTransactionIds` |
| **L2-FR-06** | Detect duplicates arriving earlier or later in batch | Checks both `requestId` and `transactionId` across buffered (`PENDING`) and settled (`PROCESSED`) sets |
| **L2-FR-07** | Out-of-order event strategy | **Option A (Sequence-Based Processing)** with priority-queue buffering and automatic cascading drain (`STREAM_SEQUENCE` and `ACCOUNT_SEQUENCE` modes) |
| **L2-FR-08** | Simulate at least one retryable failure scenario | `FailureSimulator` injects configurable transient faults (e.g., 1 failure on `TXN-2005` in Section 9.8) |
| **L2-FR-09** | Retry transient records without duplicate effects | `TransactionProcessingService.executeWithRetry` transitions `PROCESSING -> RETRY_PENDING -> PROCESSING -> PROCESSED` prior to atomic commit |
| **L2-FR-10** | Separate business logic from input handling | Clean hexagonal split across `adapter`, `domain`, `service`, `repository`, and `observability` packages |
| **L2-FR-11** | Processing summary with counts by status | `ProcessingSummary` and `ProcessingReporter` output counts per status, retry totals, missing sequences, and settled balances |
| **L2-FR-12** | Unit tests for duplicate, out-of-order, invalid, retry | `TransactionProcessingServiceTest` (JUnit 5) covers 6 comprehensive scenario suites |

## Prerequisites

* **JDK 21** (also compatible with JDK 17+)
* **Apache Maven 3.8+**

## Execution Instructions

### 1. Run Unit Tests (JUnit 5)

```bash
mvn clean test
```

### 2. Run the Application on Section 9.8 Sample Data

Using Maven Exec Plugin:

```bash
mvn compile exec:java
```

Or pass a custom JSON input file path:

```bash
mvn compile exec:java -Dexec.args="sample_data/section_9_8_input.json"
```

### 3. Zero-Dependency Direct JDK Execution (Without Maven)

Because the core service and JSON file adapter are written with zero external runtime dependencies, you can also compile and run directly with `javac` and `java`:

```bash
mkdir -p out
javac -d out $(find src/main/java -name "*.java")
java -cp out com.enterprise.fde.txprocessor.TransactionProcessorApp sample_data/section_9_8_input.json
```

## Sample Input & Output (Section 9.8 Test Data)

### Input (`sample_data/section_9_8_input.json`)

| Input Order | `transactionId` | `requestId` | `sequence` | `accountId` | `type` | `amount` | Expected Behavior |
| :---: | :--- | :--- | :---: | :--- | :--- | :---: | :--- |
| **1** | `TXN-2002` | `REQ-2` | `2` | `ACC-201` | `DEBIT` | `40` | Arrives early (`seq=2` before `seq=1`); buffered as `PENDING`, then drained & `PROCESSED` right after `REQ-1` |
| **2** | `TXN-2001` | `REQ-1` | `1` | `ACC-201` | `CREDIT` | `100` | Matches `expectedSequence=1`; `PROCESSED` immediately (`ACC-201 = 100.00`) and triggers drain of `TXN-2002` (`ACC-201 = 60.00`) |
| **3** | `TXN-2002` | `REQ-2-DUP` | `2` | `ACC-201` | `DEBIT` | `40` | Detected as `DUPLICATE` (`TXN-2002` already processed); ignored safely |
| **4** | `TXN-2003` | `REQ-3` | `3` | `ACC-201` | `CREDIT` | `25` | Matches `expectedSequence=3`; `PROCESSED` (`ACC-201 = 85.00`) |
| **5** | `TXN-2004` | `REQ-4` | `4` | `ACC-202` | `DEBIT` | `-10` | Rejected by `TransactionValidator` (`amount <= 0`); marked `FAILED` and isolated |
| **6** | `TXN-2005` | `REQ-5` | `5` | `ACC-203` | `CREDIT` | `500` | Simulates transient failure on Attempt 1 (`RETRY_PENDING`), succeeds on Attempt 2 (`PROCESSED`, `ACC-203 = 500.00`) |

### Output Summary (`sample_data/section_9_8_console_report.txt`)

```text
================================================================================
           IDEMPOTENT TRANSACTION PROCESSING SERVICE - L2 SUMMARY REPORT        
================================================================================
Total Requests Ingested       : 6
Unique Transactions Applied   : 4
Total Transient Retry Attempts: 1
--------------------------------------------------------------------------------
COUNTS BY FINAL STATUS:
  PENDING         : 0
  PROCESSED       : 4
  DUPLICATE       : 1
  FAILED          : 1
--------------------------------------------------------------------------------
PER-REQUEST LIFECYCLE OUTCOMES:
  TXN_ID     | REQ_ID     | SEQ   | ACCOUNT  | STATUS      | ATTEMPTS | DIAGNOSTIC
  -----------+------------+-------+----------+-------------+----------+---------
  TXN-2002   | REQ-2      | 2     | ***201   | PROCESSED   | 1        | Applied DEBIT 40 USD
  TXN-2001   | REQ-1      | 1     | ***201   | PROCESSED   | 1        | Applied CREDIT 100 USD
  TXN-2002   | REQ-2-DUP  | 2     | ***201   | DUPLICATE   | 0        | Duplicate detected for transactionId=TXN-2002 (requestId=REQ-2-DUP)
  TXN-2003   | REQ-3      | 3     | ***201   | PROCESSED   | 1        | Applied CREDIT 25 USD
  TXN-2004   | REQ-4      | 4     | ***202   | FAILED      | 0        | Validation failed: amount must be strictly positive (> 0), got -10
  TXN-2005   | REQ-5      | 5     | ***203   | PROCESSED   | 2        | Applied CREDIT 500 USD
--------------------------------------------------------------------------------
SETTLED ACCOUNT BALANCES:
  ACC-201         : 85.00
  ACC-203         : 500.00
================================================================================
```
