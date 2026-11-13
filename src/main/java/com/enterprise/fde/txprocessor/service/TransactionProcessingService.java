package com.enterprise.fde.txprocessor.service;

import com.enterprise.fde.txprocessor.adapter.TransactionInputSource;
import com.enterprise.fde.txprocessor.config.OrderingScope;
import com.enterprise.fde.txprocessor.config.ProcessorConfig;
import com.enterprise.fde.txprocessor.domain.ProcessingSummary;
import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import com.enterprise.fde.txprocessor.domain.TransactionStateRecord;
import com.enterprise.fde.txprocessor.domain.TransactionStatus;
import com.enterprise.fde.txprocessor.observability.StructuredAuditLogger;
import com.enterprise.fde.txprocessor.repository.IdempotencyAndStateStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Core FDE L2 background transaction processing engine implementing:
 * <ul>
 *   <li>Two-level idempotency and duplicate detection (by requestId and transactionId)</li>
 *   <li>Option A Sequence-Based Out-of-Order Buffering & Automatic Drain</li>
 *   <li>Bounded retry resilience for transient failures without duplicate side effects</li>
 *   <li>Strict validation and fault isolation for malformed records</li>
 *   <li>Structured audit logging and summary metrics generation</li>
 * </ul>
 */
public final class TransactionProcessingService {
    private static final String GLOBAL_STREAM_KEY = "__GLOBAL_STREAM__";

    private final ProcessorConfig config;
    private final IdempotencyAndStateStore store;
    private final TransactionValidator validator;
    private final FailureSimulator failureSimulator;
    private final StructuredAuditLogger auditLogger;

    private final Map<String, Long> expectedSequenceByScopeKey = new ConcurrentHashMap<>();
    private final Map<String, PriorityQueue<BufferedEntry>> pendingBuffersByScopeKey = new ConcurrentHashMap<>();
    private final AtomicInteger totalRetriesCounter = new AtomicInteger(0);

    public TransactionProcessingService(
            ProcessorConfig config,
            IdempotencyAndStateStore store,
            FailureSimulator failureSimulator,
            StructuredAuditLogger auditLogger
    ) {
        this.config = config;
        this.store = store;
        this.validator = new TransactionValidator();
        this.failureSimulator = failureSimulator;
        this.auditLogger = auditLogger;
    }

    /**
     * Ingests and processes a batch of transaction requests from the configured {@link TransactionInputSource}.
     */
    public synchronized ProcessingSummary processBatch(TransactionInputSource inputSource) {
        List<TransactionRequest> requests = inputSource != null && inputSource.fetchBatch() != null
                ? inputSource.fetchBatch()
                : List.of();
        for (TransactionRequest request : requests) {
            ingestSingleRequest(request);
        }
        return buildSummary(requests.size());
    }

    /**
     * Processes a single incoming transaction request through validation, deduplication,
     * sequence ordering, and retry-safe execution.
     */
    public synchronized TransactionStateRecord ingestSingleRequest(TransactionRequest request) {
        TransactionStateRecord record = new TransactionStateRecord(request);

        if (request == null) {
            store.registerRequestRecord(record);
            record.transitionTo(TransactionStatus.FAILED, "Validation failed: request payload is null");
            auditLogger.logEvent(
                    "VALIDATION_FAILED",
                    null,
                    null,
                    null,
                    0L,
                    TransactionStatus.FAILED,
                    0,
                    record.getDiagnosticMessage()
            );
            return record;
        }

        // 1. Check duplicate delivery by requestId or already-processed/buffered business transactionId (L2-FR-05, L2-FR-06)
        if (store.isRequestSeen(request.requestId())
                || store.isTransactionBufferedOrProcessed(request.transactionId())) {
            store.registerRequestRecord(record);
            record.transitionTo(
                    TransactionStatus.DUPLICATE,
                    "Duplicate detected for transactionId=" + request.transactionId()
                            + " (requestId=" + request.requestId() + ")"
            );
            auditLogger.logEvent(
                    "DUPLICATE_IGNORED",
                    request.transactionId(),
                    request.requestId(),
                    request.accountId(),
                    request.sequenceNumber(),
                    TransactionStatus.DUPLICATE,
                    0,
                    record.getDiagnosticMessage()
            );
            return record;
        }

        store.registerRequestRecord(record);

        // 2. Validate mandatory fields and business constraints (L2-FR-02)
        Optional<String> validationError = validator.validate(request);
        if (validationError.isPresent()) {
            record.transitionTo(TransactionStatus.FAILED, validationError.get());
            auditLogger.logEvent(
                    "VALIDATION_FAILED",
                    request.transactionId(),
                    request.requestId(),
                    request.accountId(),
                    request.sequenceNumber(),
                    TransactionStatus.FAILED,
                    0,
                    validationError.get()
            );
            // If sequence cursor is aligned with this malformed record, advance cursor so subsequent valid records aren't blocked
            if (config.advanceSequenceOnInvalidRecord() && request.sequenceNumber() > 0) {
                String scopeKey = resolveScopeKey(request);
                long expected = getExpectedSequence(scopeKey);
                if (request.sequenceNumber() == expected) {
                    setExpectedSequence(scopeKey, expected + 1);
                    drainReadyBuffer(scopeKey);
                }
            }
            return record;
        }

        // 3. Evaluate Option A Sequence-Based Ordering (L2-FR-07, Section 9.5)
        String scopeKey = resolveScopeKey(request);
        long expectedSequence = getExpectedSequence(scopeKey);
        long incomingSequence = request.sequenceNumber();

        if (incomingSequence > expectedSequence) {
            // Out-of-order future sequence: hold in PENDING buffer until prior sequences arrive
            store.markTransactionBuffered(request.transactionId());
            record.transitionTo(
                    TransactionStatus.PENDING,
                    "Buffered out-of-order request: sequence=" + incomingSequence
                            + " awaiting expectedSequence=" + expectedSequence
            );
            auditLogger.logEvent(
                    "BUFFERED_OUT_OF_ORDER",
                    request.transactionId(),
                    request.requestId(),
                    request.accountId(),
                    incomingSequence,
                    TransactionStatus.PENDING,
                    0,
                    record.getDiagnosticMessage()
            );
            getPendingQueue(scopeKey).add(new BufferedEntry(request, record));
            return record;
        }

        if (incomingSequence < expectedSequence) {
            // Stale / past sequence that was not seen in idempotency store (e.g. conflicting sequence number reuse)
            record.transitionTo(
                    TransactionStatus.FAILED,
                    "Stale or conflicting sequenceNumber=" + incomingSequence
                            + " (expected >= " + expectedSequence + ")"
            );
            auditLogger.logEvent(
                    "STALE_SEQUENCE_REJECTED",
                    request.transactionId(),
                    request.requestId(),
                    request.accountId(),
                    incomingSequence,
                    TransactionStatus.FAILED,
                    0,
                    record.getDiagnosticMessage()
            );
            return record;
        }

        // 4. Incoming sequence matches expectedSequence -> execute with retry resilience and drain buffered followers
        executeWithRetry(request, record);
        setExpectedSequence(scopeKey, expectedSequence + 1);
        drainReadyBuffer(scopeKey);
        return record;
    }

    private void drainReadyBuffer(String scopeKey) {
        PriorityQueue<BufferedEntry> queue = getPendingQueue(scopeKey);
        while (!queue.isEmpty()) {
            long expected = getExpectedSequence(scopeKey);
            BufferedEntry head = queue.peek();
            if (head.request().sequenceNumber() == expected) {
                queue.poll();
                executeWithRetry(head.request(), head.record());
                setExpectedSequence(scopeKey, expected + 1);
            } else if (head.request().sequenceNumber() < expected) {
                // Sequence already passed while buffered (e.g. two buffered requests shared the same sequence number)
                queue.poll();
                store.unmarkTransactionBuffered(head.request().transactionId());
                if (store.isTransactionProcessed(head.request().transactionId())) {
                    head.record().transitionTo(
                            TransactionStatus.DUPLICATE,
                            "Buffered request superseded by earlier duplicate execution"
                    );
                    auditLogger.logEvent(
                            "SUPERSEDED_DUPLICATE",
                            head.request().transactionId(),
                            head.request().requestId(),
                            head.request().accountId(),
                            head.request().sequenceNumber(),
                            TransactionStatus.DUPLICATE,
                            head.record().getAttemptCount(),
                            head.record().getDiagnosticMessage()
                    );
                } else {
                    head.record().transitionTo(
                            TransactionStatus.FAILED,
                            "Buffered sequence conflict at sequence=" + head.request().sequenceNumber()
                    );
                    auditLogger.logEvent(
                            "BUFFERED_SEQUENCE_CONFLICT",
                            head.request().transactionId(),
                            head.request().requestId(),
                            head.request().accountId(),
                            head.request().sequenceNumber(),
                            TransactionStatus.FAILED,
                            head.record().getAttemptCount(),
                            head.record().getDiagnosticMessage()
                    );
                }
            } else {
                // Still waiting for an intermediate missing sequence
                break;
            }
        }
    }

    /**
     * Executes a single validated, in-order transaction with bounded retry handling (L2-FR-08, L2-FR-09, Section 9.6).
     */
    private void executeWithRetry(TransactionRequest request, TransactionStateRecord record) {
        int maxAttempts = config.maxRetries() + 1;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            record.incrementAttempt();
            record.transitionTo(
                    TransactionStatus.PROCESSING,
                    "Processing attempt " + attempt + " of " + maxAttempts
            );
            auditLogger.logEvent(
                    "PROCESSING_STARTED",
                    request.transactionId(),
                    request.requestId(),
                    request.accountId(),
                    request.sequenceNumber(),
                    TransactionStatus.PROCESSING,
                    attempt,
                    record.getDiagnosticMessage()
            );

            try {
                // Check simulated transient failures (e.g. network/DB lock timeout) BEFORE committing state
                failureSimulator.maybeThrowTransientFailure(request, attempt);

                // Commit balance update + idempotency registration atomically
                store.applyTransactionAtomically(request, record, config.allowNegativeAccountBalance());
                auditLogger.logEvent(
                        "TRANSACTION_PROCESSED",
                        request.transactionId(),
                        request.requestId(),
                        request.accountId(),
                        request.sequenceNumber(),
                        record.getStatus(),
                        attempt,
                        record.getDiagnosticMessage()
                );
                return;
            } catch (FailureSimulator.TransientProcessingException transientEx) {
                if (attempt < maxAttempts) {
                    totalRetriesCounter.incrementAndGet();
                    record.transitionTo(
                            TransactionStatus.RETRY_PENDING,
                            transientEx.getMessage() + " -> scheduling retry"
                    );
                    auditLogger.logEvent(
                            "TRANSIENT_FAILURE_RETRY_SCHEDULED",
                            request.transactionId(),
                            request.requestId(),
                            request.accountId(),
                            request.sequenceNumber(),
                            TransactionStatus.RETRY_PENDING,
                            attempt,
                            record.getDiagnosticMessage()
                    );
                } else {
                    store.unmarkTransactionBuffered(request.transactionId());
                    record.transitionTo(
                            TransactionStatus.FAILED,
                            "Retry exhausted after " + attempt + " attempts: " + transientEx.getMessage()
                    );
                    auditLogger.logEvent(
                            "RETRY_EXHAUSTED_FAILED",
                            request.transactionId(),
                            request.requestId(),
                            request.accountId(),
                            request.sequenceNumber(),
                            TransactionStatus.FAILED,
                            attempt,
                            record.getDiagnosticMessage()
                    );
                }
            } catch (Exception businessOrFatalEx) {
                store.unmarkTransactionBuffered(request.transactionId());
                record.transitionTo(
                        TransactionStatus.FAILED,
                        "Non-retryable processing failure: " + businessOrFatalEx.getMessage()
                );
                auditLogger.logEvent(
                        "NON_RETRYABLE_FAILURE",
                        request.transactionId(),
                        request.requestId(),
                        request.accountId(),
                        request.sequenceNumber(),
                        TransactionStatus.FAILED,
                        attempt,
                        record.getDiagnosticMessage()
                );
                return;
            }
        }
    }

    private ProcessingSummary buildSummary(int totalReceived) {
        List<TransactionStateRecord> records = store.getAllRequestRecords();
        Map<TransactionStatus, Integer> statusCounts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus st : TransactionStatus.values()) {
            statusCounts.put(st, 0);
        }
        for (TransactionStateRecord r : records) {
            statusCounts.compute(r.getStatus(), (k, v) -> (v == null ? 0 : v) + 1);
        }

        List<Long> missingSequences = new ArrayList<>();
        for (Map.Entry<String, PriorityQueue<BufferedEntry>> entry : pendingBuffersByScopeKey.entrySet()) {
            PriorityQueue<BufferedEntry> q = entry.getValue();
            if (!q.isEmpty()) {
                long expected = getExpectedSequence(entry.getKey());
                Set<Long> bufferedSeqs = q.stream()
                        .map(e -> e.request().sequenceNumber())
                        .collect(Collectors.toSet());
                long maxBuffered = bufferedSeqs.stream().mapToLong(Long::longValue).max().orElse(expected);
                for (long seq = expected; seq < maxBuffered; seq++) {
                    if (!bufferedSeqs.contains(seq)) {
                        missingSequences.add(seq);
                    }
                }
            }
        }

        return new ProcessingSummary(
                totalReceived,
                statusCounts,
                store.getUniqueProcessedTransactionCount(),
                totalRetriesCounter.get(),
                missingSequences,
                store.snapshotAccountBalances(),
                records
        );
    }

    private String resolveScopeKey(TransactionRequest request) {
        if (config.orderingScope() == OrderingScope.ACCOUNT_SEQUENCE) {
            return request.accountId() == null ? GLOBAL_STREAM_KEY : request.accountId().trim();
        }
        return GLOBAL_STREAM_KEY;
    }

    private long getExpectedSequence(String scopeKey) {
        return expectedSequenceByScopeKey.getOrDefault(scopeKey, 1L);
    }

    private void setExpectedSequence(String scopeKey, long nextExpected) {
        expectedSequenceByScopeKey.put(scopeKey, nextExpected);
    }

    private PriorityQueue<BufferedEntry> getPendingQueue(String scopeKey) {
        return pendingBuffersByScopeKey.computeIfAbsent(
                scopeKey,
                k -> new PriorityQueue<>(Comparator.comparingLong(e -> e.request().sequenceNumber()))
        );
    }

    private record BufferedEntry(TransactionRequest request, TransactionStateRecord record) {
    }
}
