package com.enterprise.fde.txprocessor.observability;

import com.enterprise.fde.txprocessor.domain.TransactionStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Emits structured JSON log events while masking sensitive account identifiers (Section 6.5, 6.6).
 */
public final class StructuredAuditLogger {
    private final List<String> logEntries = Collections.synchronizedList(new ArrayList<>());
    private final boolean consoleEcho;

    public StructuredAuditLogger(boolean consoleEcho) {
        this.consoleEcho = consoleEcho;
    }

    public void logEvent(
            String eventType,
            String transactionId,
            String requestId,
            String accountId,
            long sequenceNumber,
            TransactionStatus status,
            int attempt,
            String detail
    ) {
        String maskedAccount = maskAccountId(accountId);
        String jsonLine = String.format(
                "{\"timestamp\":\"%s\",\"event\":\"%s\",\"transactionId\":\"%s\",\"requestId\":\"%s\","
                        + "\"accountRef\":\"%s\",\"sequence\":%d,\"status\":\"%s\",\"attempt\":%d,\"detail\":\"%s\"}",
                Instant.now().toString(),
                escape(eventType),
                escape(transactionId),
                escape(requestId),
                escape(maskedAccount),
                sequenceNumber,
                status != null ? status.name() : "UNKNOWN",
                attempt,
                escape(detail)
        );
        logEntries.add(jsonLine);
        if (consoleEcho) {
            System.out.println(jsonLine);
        }
    }

    /**
     * Redacts raw customer account identifier to prevent sensitive data leakage in logs (Section 6.6).
     */
    public static String maskAccountId(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            return "REDACTED";
        }
        String trimmed = accountId.trim();
        if (trimmed.length() <= 3) {
            return "***";
        }
        return "***" + trimmed.substring(trimmed.length() - 3);
    }

    public List<String> getLogEntries() {
        synchronized (logEntries) {
            return List.copyOf(logEntries);
        }
    }

    private static String escape(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
