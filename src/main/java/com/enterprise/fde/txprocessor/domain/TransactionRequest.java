package com.enterprise.fde.txprocessor.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Incoming business transaction request payload (Section 5).
 *
 * @param transactionId  unique business transaction identifier (used for business idempotency)
 * @param requestId      unique delivery/request identifier (used for retry & duplicate tracking)
 * @param accountId      customer or ledger account identifier
 * @param transactionType CREDIT, DEBIT, or REVERSAL
 * @param amount         monetary transaction amount (must be > 0)
 * @param currency       ISO-4217 currency code (defaults to USD if omitted in legacy feeds)
 * @param sequenceNumber monotonic sequence number used for out-of-order detection
 * @param eventTime      optional producer timestamp
 */
public record TransactionRequest(
        String transactionId,
        String requestId,
        String accountId,
        TransactionType transactionType,
        BigDecimal amount,
        String currency,
         long sequenceNumber,
        Instant eventTime
) {
    public TransactionRequest(
            String transactionId,
            String requestId,
            String accountId,
            TransactionType transactionType,
            BigDecimal amount,
            long sequenceNumber
    ) {
        this(transactionId, requestId, accountId, transactionType, amount, "USD", sequenceNumber, Instant.now());
    }

    public String normalizedCurrency() {
        return (currency == null || currency.isBlank()) ? "USD" : currency.trim().toUpperCase();
    }
}
