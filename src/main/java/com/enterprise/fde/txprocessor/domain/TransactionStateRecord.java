package com.enterprise.fde.txprocessor.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mutable internal state record with immutable snapshot views for auditability and lifecycle tracking.
 */
public final class TransactionStateRecord {
    private final String requestId;
    private final String transactionId;
    private final String accountId;
    private final TransactionType transactionType;
    private final BigDecimal amount;
    private final String currency;
    private final long sequenceNumber;
    private TransactionStatus status;
    private int attemptCount;
    private String diagnosticMessage;
    private final List<StatusTransition> transitions = new ArrayList<>();

    public TransactionStateRecord(TransactionRequest request) {
        this.requestId = request != null ? request.requestId() : null;
        this.transactionId = request != null ? request.transactionId() : null;
        this.accountId = request != null ? request.accountId() : null;
        this.transactionType = request != null ? request.transactionType() : null;
        this.amount = request != null ? request.amount() : null;
        this.currency = request != null ? request.normalizedCurrency() : "USD";
        this.sequenceNumber = request != null ? request.sequenceNumber() : 0L;
        this.status = TransactionStatus.RECEIVED;
        this.attemptCount = 0;
        this.diagnosticMessage = "Request received";
        this.transitions.add(new StatusTransition(null, TransactionStatus.RECEIVED, "Ingested", Instant.now()));
    }

    public synchronized void transitionTo(TransactionStatus nextStatus, String reason) {
        TransactionStatus previous = this.status;
        this.status = nextStatus;
        this.diagnosticMessage = reason;
        this.transitions.add(new StatusTransition(previous, nextStatus, reason, Instant.now()));
    }

    public synchronized void incrementAttempt() {
        this.attemptCount++;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public synchronized TransactionStatus getStatus() {
        return status;
    }

    public synchronized int getAttemptCount() {
        return attemptCount;
    }

    public synchronized String getDiagnosticMessage() {
        return diagnosticMessage;
    }

    public synchronized List<StatusTransition> getTransitions() {
        return Collections.unmodifiableList(new ArrayList<>(transitions));
    }
}
