package com.enterprise.fde.txprocessor;

import com.enterprise.fde.txprocessor.adapter.InMemoryTransactionInputSource;
import com.enterprise.fde.txprocessor.adapter.JsonFileTransactionInputSource;
import com.enterprise.fde.txprocessor.adapter.TransactionInputSource;
import com.enterprise.fde.txprocessor.config.ProcessorConfig;
import com.enterprise.fde.txprocessor.domain.ProcessingSummary;
import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import com.enterprise.fde.txprocessor.domain.TransactionType;
import com.enterprise.fde.txprocessor.observability.ProcessingReporter;
import com.enterprise.fde.txprocessor.observability.StructuredAuditLogger;
import com.enterprise.fde.txprocessor.repository.InMemoryIdempotencyAndStateStore;
import com.enterprise.fde.txprocessor.service.FailureSimulator;
import com.enterprise.fde.txprocessor.service.TransactionProcessingService;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * CLI entry point running the Section 9.8 L2 Suggested Test Data batch and printing
 * structured JSON logs followed by the operational summary report.
 */
public final class TransactionProcessorApp {

    public static void main(String[] args) {
        TransactionInputSource inputSource;
        Path defaultSampleFile = Path.of("sample_data", "section_9_8_input.json");

        if (args.length > 0 && Files.exists(Path.of(args[0]))) {
            inputSource = new JsonFileTransactionInputSource(Path.of(args[0]));
        } else if (Files.exists(defaultSampleFile)) {
            inputSource = new JsonFileTransactionInputSource(defaultSampleFile);
        } else {
            inputSource = new InMemoryTransactionInputSource(section98SampleRequests());
        }

        ProcessorConfig config = ProcessorConfig.defaultL2Config();
        InMemoryIdempotencyAndStateStore store = new InMemoryIdempotencyAndStateStore();
        // Simulate 1 transient failure on TXN-2005 as specified in Section 9.8 Row 6
        FailureSimulator failureSimulator = new FailureSimulator().failTimes("TXN-2005", 1);
        StructuredAuditLogger logger = new StructuredAuditLogger(true);

        TransactionProcessingService service = new TransactionProcessingService(
                config,
                store,
                failureSimulator,
                logger
        );

        System.out.println("--- STREAMING STRUCTURED AUDIT LOGS ---");
        ProcessingSummary summary = service.processBatch(inputSource);
        System.out.println();

        ProcessingReporter reporter = new ProcessingReporter();
        System.out.print(reporter.formatConsoleReport(summary));
    }

    public static List<TransactionRequest> section98SampleRequests() {
        return List.of(
                new TransactionRequest("TXN-2002", "REQ-2", "ACC-201", TransactionType.DEBIT, new BigDecimal("40"), 2L),
                new TransactionRequest("TXN-2001", "REQ-1", "ACC-201", TransactionType.CREDIT, new BigDecimal("100"), 1L),
                new TransactionRequest("TXN-2002", "REQ-2-DUP", "ACC-201", TransactionType.DEBIT, new BigDecimal("40"), 2L),
                new TransactionRequest("TXN-2003", "REQ-3", "ACC-201", TransactionType.CREDIT, new BigDecimal("25"), 3L),
                new TransactionRequest("TXN-2004", "REQ-4", "ACC-202", TransactionType.DEBIT, new BigDecimal("-10"), 4L),
                new TransactionRequest("TXN-2005", "REQ-5", "ACC-203", TransactionType.CREDIT, new BigDecimal("500"), 5L)
        );
    }
}
