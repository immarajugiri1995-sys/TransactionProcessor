package com.enterprise.fde.txprocessor.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Operational visibility summary produced after batch execution (L2-FR-11, Section 6.5).
 *
 * @param totalRequestsReceived  total input items read from the source
 * @param countsByStatus         breakdown of final record statuses (PROCESSED, DUPLICATE, FAILED, PENDING, etc.)
 * @param uniqueTransactionsApplied number of unique business transactions that mutated account state
 * @param totalRetryAttempts     total retry executions triggered by transient failures
 * @param missingSequences       any missing sequence numbers that left buffered items in PENDING state
 * @param finalAccountBalances   map of accountId to final settled balance
 * @param requestOutcomes        ordered list of per-request state records for audit verification
 */
public record ProcessingSummary(
        int totalRequestsReceived,
        Map<TransactionStatus, Integer> countsByStatus,
        int uniqueTransactionsApplied,
        int totalRetryAttempts,
        List<Long> missingSequences,
        Map<String, BigDecimal> finalAccountBalances,
        List<TransactionStateRecord> requestOutcomes
) {
    public int countFor(TransactionStatus status) {
        return countsByStatus.getOrDefault(status, 0);
    }
}
