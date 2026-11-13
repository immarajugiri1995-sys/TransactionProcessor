package com.enterprise.fde.txprocessor;

import com.enterprise.fde.txprocessor.adapter.InMemoryTransactionInputSource;
import com.enterprise.fde.txprocessor.adapter.JsonFileTransactionInputSource;
import com.enterprise.fde.txprocessor.config.OrderingScope;
import com.enterprise.fde.txprocessor.config.ProcessorConfig;
import com.enterprise.fde.txprocessor.domain.ProcessingSummary;
import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import com.enterprise.fde.txprocessor.domain.TransactionStateRecord;
import com.enterprise.fde.txprocessor.domain.TransactionStatus;
import com.enterprise.fde.txprocessor.domain.TransactionType;
import com.enterprise.fde.txprocessor.observability.StructuredAuditLogger;
import com.enterprise.fde.txprocessor.repository.InMemoryIdempotencyAndStateStore;
import com.enterprise.fde.txprocessor.service.FailureSimulator;
import com.enterprise.fde.txprocessor.service.TransactionProcessingService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive JUnit 5 test suite validating all FDE L2 Functional Requirements (L2-FR-01 to L2-FR-12)
 * and the exact Section 9.8 Suggested Test Data scenarios.
 */
class TransactionProcessingServiceTest {

    @Test
    @DisplayName("Section 9.8 End-to-End Batch: Out-of-order buffer, duplicate detection, invalid isolation, and retry recovery")
    void shouldProcessSection98SuggestedTestDataCorrectly() {
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        FailureSimulator simulator = new FailureSimulator().failTimes("TXN-2005", 1);
        StructuredAuditLogger logger = new StructuredAuditLogger(false);

        TransactionProcessingService service = new TransactionProcessingService(
                ProcessorConfig.defaultL2Config(),
                store,
                simulator,
                logger
        );

        List<TransactionRequest> batch = TransactionProcessorApp.section98SampleRequests();
        ProcessingSummary summary = service.processBatch(new InMemoryTransactionInputSource(batch));

        // Total 6 input requests:
        // - 4 PROCESSED (TXN-2002 via REQ-2, TXN-2001 via REQ-1, TXN-2003 via REQ-3, TXN-2005 via REQ-5 after 1 retry)
        // - 1 DUPLICATE (TXN-2002 via REQ-2-DUP)
        // - 1 FAILED (TXN-2004 via REQ-4 with negative amount -10)
        assertEquals(6, summary.totalRequestsReceived());
        assertEquals(4, summary.countFor(TransactionStatus.PROCESSED));
        assertEquals(1, summary.countFor(TransactionStatus.DUPLICATE));
        assertEquals(1, summary.countFor(TransactionStatus.FAILED));
        assertEquals(0, summary.countFor(TransactionStatus.PENDING));
        assertEquals(4, summary.uniqueTransactionsApplied());
        assertEquals(1, summary.totalRetryAttempts());

        // Verify ACC-201 balance: +100 (TXN-2001) - 40 (TXN-2002) + 25 (TXN-2003) = 85.00
        assertEquals(new BigDecimal("85.00"), summary.finalAccountBalances().get("ACC-201"));
        // Verify ACC-203 balance: +500.00 (TXN-2005 applied exactly once despite retry)
        assertEquals(new BigDecimal("500.00"), summary.finalAccountBalances().get("ACC-203"));
        // Verify ACC-202 was never created/mutated because TXN-2004 failed validation
        assertFalse(summary.finalAccountBalances().containsKey("ACC-202"));

        // Verify TXN-2002 (REQ-2) lifecycle transitioned through RECEIVED -> PENDING -> PROCESSING -> PROCESSED
        TransactionStateRecord req2Record = summary.requestOutcomes().get(0);
        assertEquals("REQ-2", req2Record.getRequestId());
        assertEquals(TransactionStatus.PROCESSED, req2Record.getStatus());
        List<TransactionStatus> req2States = req2Record.getTransitions().stream()
                .map(t -> t.toStatus())
                .toList();
        assertEquals(
                List.of(TransactionStatus.RECEIVED, TransactionStatus.PENDING, TransactionStatus.PROCESSING, TransactionStatus.PROCESSED),
                req2States
        );

        // Verify TXN-2005 (REQ-5) lifecycle transitioned through RECEIVED -> PROCESSING -> RETRY_PENDING -> PROCESSING -> PROCESSED
        TransactionStateRecord req5Record = summary.requestOutcomes().get(5);
        assertEquals("REQ-5", req5Record.getRequestId());
        assertEquals(TransactionStatus.PROCESSED, req5Record.getStatus());
        assertEquals(2, req5Record.getAttemptCount());
        List<TransactionStatus> req5States = req5Record.getTransitions().stream()
                .map(t -> t.toStatus())
                .toList();
        assertEquals(
                List.of(
                        TransactionStatus.RECEIVED,
                        TransactionStatus.PROCESSING,
                        TransactionStatus.RETRY_PENDING,
                        TransactionStatus.PROCESSING,
                        TransactionStatus.PROCESSED
                ),
                req5States
        );
    }

    @Test
    @DisplayName("L2-FR-05 & L2-FR-06: Duplicate detection when duplicate arrives while original is still PENDING in buffer")
    void shouldDetectDuplicateEvenWhileOriginalRequestIsPendingInBuffer() {
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        TransactionProcessingService service = new TransactionProcessingService(
                ProcessorConfig.defaultL2Config(),
                store,
                new FailureSimulator(),
                new StructuredAuditLogger(false)
        );

        // Sequence 2 arrives before Sequence 1, followed immediately by a duplicate of Sequence 2
        List<TransactionRequest> requests = List.of(
                new TransactionRequest("TXN-3002", "REQ-A", "ACC-301", TransactionType.CREDIT, new BigDecimal("50"), 2L),
                new TransactionRequest("TXN-3002", "REQ-A-DUP", "ACC-301", TransactionType.CREDIT, new BigDecimal("50"), 2L),
                new TransactionRequest("TXN-3001", "REQ-B", "ACC-301", TransactionType.CREDIT, new BigDecimal("100"), 1L)
        );

        ProcessingSummary summary = service.processBatch(new InMemoryTransactionInputSource(requests));

        assertEquals(2, summary.countFor(TransactionStatus.PROCESSED));
        assertEquals(1, summary.countFor(TransactionStatus.DUPLICATE));
        assertEquals(new BigDecimal("150.00"), summary.finalAccountBalances().get("ACC-301"));
    }

    @Test
    @DisplayName("L2-FR-08 & L2-FR-09: Exhausted retries transition transaction to FAILED without mutating account balance")
    void shouldMarkTransactionFailedWhenRetriesExhausted() {
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        // Configure 5 transient failures, exceeding maxRetries = 2 (3 total attempts)
        FailureSimulator simulator = new FailureSimulator().failTimes("TXN-FAIL-99", 5);
        ProcessorConfig config = new ProcessorConfig(2, OrderingScope.STREAM_SEQUENCE, true, false);

        TransactionProcessingService service = new TransactionProcessingService(
                config,
                store,
                simulator,
                new StructuredAuditLogger(false)
        );

        TransactionRequest req = new TransactionRequest(
                "TXN-FAIL-99", "REQ-99", "ACC-999", TransactionType.CREDIT, new BigDecimal("250"), 1L
        );
        ProcessingSummary summary = service.processBatch(new InMemoryTransactionInputSource(List.of(req)));

        assertEquals(0, summary.countFor(TransactionStatus.PROCESSED));
        assertEquals(1, summary.countFor(TransactionStatus.FAILED));
        assertEquals(2, summary.totalRetryAttempts());
        assertEquals(3, summary.requestOutcomes().get(0).getAttemptCount());
        assertFalse(summary.finalAccountBalances().containsKey("ACC-999"));
    }

    @Test
    @DisplayName("L2-FR-07: Missing sequence gap leaves future transaction in PENDING state and reports missing sequence")
    void shouldReportMissingSequenceWhenGapRemainsAtEndOfBatch() {
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        TransactionProcessingService service = new TransactionProcessingService(
                ProcessorConfig.defaultL2Config(),
                store,
                new FailureSimulator(),
                new StructuredAuditLogger(false)
        );

        // Sequence 1 and Sequence 3 arrive; Sequence 2 is missing
        List<TransactionRequest> requests = List.of(
                new TransactionRequest("TXN-1", "REQ-1", "ACC-1", TransactionType.CREDIT, new BigDecimal("100"), 1L),
                new TransactionRequest("TXN-3", "REQ-3", "ACC-1", TransactionType.DEBIT, new BigDecimal("30"), 3L)
        );

        ProcessingSummary summary = service.processBatch(new InMemoryTransactionInputSource(requests));

        assertEquals(1, summary.countFor(TransactionStatus.PROCESSED));
        assertEquals(1, summary.countFor(TransactionStatus.PENDING));
        assertEquals(List.of(2L), summary.missingSequences());
        assertEquals(new BigDecimal("100.00"), summary.finalAccountBalances().get("ACC-1"));
    }

    @Test
    @DisplayName("Per-Account Sequence Ordering Mode: Independent sequences across multiple accounts")
    void shouldSupportIndependentPerAccountSequenceOrdering() {
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        TransactionProcessingService service = new TransactionProcessingService(
                ProcessorConfig.perAccountConfig(2),
                store,
                new FailureSimulator(),
                new StructuredAuditLogger(false)
        );

        List<TransactionRequest> requests = List.of(
                new TransactionRequest("A-TXN-2", "A-REQ-2", "ACC-A", TransactionType.DEBIT, new BigDecimal("20"), 2L),
                new TransactionRequest("B-TXN-1", "B-REQ-1", "ACC-B", TransactionType.CREDIT, new BigDecimal("300"), 1L),
                new TransactionRequest("A-TXN-1", "A-REQ-1", "ACC-A", TransactionType.CREDIT, new BigDecimal("100"), 1L)
        );

        ProcessingSummary summary = service.processBatch(new InMemoryTransactionInputSource(requests));

        assertEquals(3, summary.countFor(TransactionStatus.PROCESSED));
        assertEquals(new BigDecimal("80.00"), summary.finalAccountBalances().get("ACC-A"));
        assertEquals(new BigDecimal("300.00"), summary.finalAccountBalances().get("ACC-B"));
    }

    @Test
    @DisplayName("L2-FR-01 & Section 6.6: JSON file input adapter and PII masking in audit logs")
    void shouldLoadFromJsonFileAndMaskSensitiveAccountIdInLogs() {
        Path samplePath = Path.of("sample_data", "section_9_8_input.json");
        JsonFileTransactionInputSource fileSource = new JsonFileTransactionInputSource(samplePath);
        List<TransactionRequest> loaded = fileSource.fetchBatch();
        assertEquals(6, loaded.size());

        StructuredAuditLogger logger = new StructuredAuditLogger(false);
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        TransactionProcessingService service = new TransactionProcessingService(
                ProcessorConfig.defaultL2Config(),
                store,
                new FailureSimulator().failTimes("TXN-2005", 1),
                logger
        );

        service.processBatch(fileSource);
        assertFalse(logger.getLogEntries().isEmpty());
        for (String logLine : logger.getLogEntries()) {
            // Raw account IDs like ACC-201 must be masked as ***201 in logs (Section 6.6)
            assertFalse(logLine.contains("\"ACC-201\""));
            assertFalse(logLine.contains("\"ACC-202\""));
            assertFalse(logLine.contains("\"ACC-203\""));
        }
        assertTrue(logger.getLogEntries().get(0).contains("***201"));
    }
}
