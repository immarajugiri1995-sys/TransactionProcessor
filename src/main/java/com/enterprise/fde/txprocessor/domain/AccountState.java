package com.enterprise.fde.txprocessor.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents an account ledger state updated atomically during transaction execution (Section 6.4).
 */
public final class AccountState {
    private final String accountId;
    private BigDecimal balance;
    private long lastProcessedSequence;
    private final List<String> appliedTransactionIds = new ArrayList<>();

    public AccountState(String accountId) {
        this.accountId = accountId;
        this.balance = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        this.lastProcessedSequence = 0L;
    }

    public synchronized void apply(TransactionRequest request, boolean allowNegativeBalance) {
        BigDecimal scaledAmount = request.amount().setScale(2, RoundingMode.HALF_UP);
        BigDecimal candidateBalance = switch (request.transactionType()) {
            case CREDIT -> this.balance.add(scaledAmount);
            case DEBIT, REVERSAL -> this.balance.subtract(scaledAmount);
        };
        if (!allowNegativeBalance && candidateBalance.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalStateException(
                    "Insufficient funds on account for " + request.transactionType()
            );
        }
        this.balance = candidateBalance;
        if (request.sequenceNumber() > this.lastProcessedSequence) {
            this.lastProcessedSequence = request.sequenceNumber();
        }
        this.appliedTransactionIds.add(request.transactionId());
    }

    public String getAccountId() {
        return accountId;
    }

    public synchronized BigDecimal getBalance() {
        return balance;
    }

    public synchronized long getLastProcessedSequence() {
        return lastProcessedSequence;
    }

    public synchronized void setLastProcessedSequence(long sequence) {
        if (sequence > this.lastProcessedSequence) {
            this.lastProcessedSequence = sequence;
        }
    }

    public synchronized List<String> getAppliedTransactionIds() {
        return Collections.unmodifiableList(new ArrayList<>(appliedTransactionIds));
    }
}
