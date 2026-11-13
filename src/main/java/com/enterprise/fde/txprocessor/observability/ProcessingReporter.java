package com.enterprise.fde.txprocessor.observability;

import com.enterprise.fde.txprocessor.domain.ProcessingSummary;
import com.enterprise.fde.txprocessor.domain.TransactionStateRecord;
import com.enterprise.fde.txprocessor.domain.TransactionStatus;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Formats operational summaries into human-readable reports and JSON snapshots (L2-FR-11, Section 6.5).
 */
public final class ProcessingReporter {

    public String formatConsoleReport(ProcessingSummary summary) {
        StringBuilder sb = new StringBuilder();
        sb.append("================================================================================\n");
        sb.append("           IDEMPOTENT TRANSACTION PROCESSING SERVICE - L2 SUMMARY REPORT        \n");
        sb.append("================================================================================\n");
        sb.append(String.format("Total Requests Ingested       : %d%n", summary.totalRequestsReceived()));
        sb.append(String.format("Unique Transactions Applied   : %d%n", summary.uniqueTransactionsApplied()));
        sb.append(String.format("Total Transient Retry Attempts: %d%n", summary.totalRetryAttempts()));
        sb.append("--------------------------------------------------------------------------------\n");
        sb.append("COUNTS BY FINAL STATUS:\n");
        for (TransactionStatus status : TransactionStatus.values()) {
            int count = summary.countFor(status);
            if (count > 0 || status == TransactionStatus.PROCESSED
                    || status == TransactionStatus.DUPLICATE
                    || status == TransactionStatus.FAILED
                    || status == TransactionStatus.PENDING) {
                sb.append(String.format("  %-15s : %d%n", status.name(), count));
            }
        }
        sb.append("--------------------------------------------------------------------------------\n");
        sb.append("PER-REQUEST LIFECYCLE OUTCOMES:\n");
        sb.append(String.format("  %-10s | %-10s | %-5s | %-8s | %-11s | %-8s | %s%n",
                "TXN_ID", "REQ_ID", "SEQ", "ACCOUNT", "STATUS", "ATTEMPTS", "DIAGNOSTIC"));
        sb.append("  -----------+------------+-------+----------+-------------+----------+---------\n");
        for (TransactionStateRecord rec : summary.requestOutcomes()) {
            sb.append(String.format("  %-10s | %-10s | %-5d | %-8s | %-11s | %-8d | %s%n",
                    nullSafe(rec.getTransactionId()),
                    nullSafe(rec.getRequestId()),
                    rec.getSequenceNumber(),
                    StructuredAuditLogger.maskAccountId(rec.getAccountId()),
                    rec.getStatus().name(),
                    rec.getAttemptCount(),
                    nullSafe(rec.getDiagnosticMessage())));
        }
        sb.append("--------------------------------------------------------------------------------\n");
        sb.append("SETTLED ACCOUNT BALANCES:\n");
        for (Map.Entry<String, BigDecimal> entry : summary.finalAccountBalances().entrySet()) {
            sb.append(String.format("  %-15s : %s%n", entry.getKey(), entry.getValue().toPlainString()));
        }
        if (!summary.missingSequences().isEmpty()) {
            sb.append("--------------------------------------------------------------------------------\n");
            sb.append("UNRESOLVED MISSING SEQUENCES  : ").append(summary.missingSequences()).append("\n");
        }
        sb.append("================================================================================\n");
        return sb.toString();
    }

    private static String nullSafe(String s) {
        return s == null ? "NULL" : s;
    }
}
