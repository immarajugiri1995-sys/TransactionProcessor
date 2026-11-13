package com.enterprise.fde.txprocessor.service;

import com.enterprise.fde.txprocessor.domain.TransactionRequest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deterministic transient failure simulator for resilience verification (L2-FR-08, L2-FR-09, Section 9.6).
 * Can be configured to fail a specific transactionId for N attempts before succeeding (or exhausting retries).
 */
public final class FailureSimulator {
    private final Map<String, Integer> remainingFailuresByTxnId = new ConcurrentHashMap<>();

    /**
     * Configures a transactionId to throw a transient exception for the first {@code failuresToTrigger} attempts.
     */
    public FailureSimulator failTimes(String transactionId, int failuresToTrigger) {
        if (failuresToTrigger > 0) {
            remainingFailuresByTxnId.put(transactionId, failuresToTrigger);
        }
        return this;
    }

    /**
     * Checks if the current attempt for {@code request} should simulate a transient infrastructure fault.
     */
    public void maybeThrowTransientFailure(TransactionRequest request, int currentAttempt) {
        Integer remaining = remainingFailuresByTxnId.get(request.transactionId());
        if (remaining != null && remaining > 0) {
            remainingFailuresByTxnId.put(request.transactionId(), remaining - 1);
            throw new TransientProcessingException(
                    "Simulated transient ledger timeout on attempt " + currentAttempt
                            + " for transactionId=" + request.transactionId()
            );
        }
    }

    public static final class TransientProcessingException extends RuntimeException {
        public TransientProcessingException(String message) {
            super(message);
        }
    }
}
