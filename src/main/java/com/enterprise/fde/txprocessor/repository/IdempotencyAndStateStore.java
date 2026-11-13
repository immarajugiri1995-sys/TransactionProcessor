package com.enterprise.fde.txprocessor.repository;

import com.enterprise.fde.txprocessor.domain.AccountState;
import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import com.enterprise.fde.txprocessor.domain.TransactionStateRecord;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstraction for the idempotency store, transaction state store, and account ledger (Section 6.1, 6.4).
 * In production, backed by PostgreSQL/CockroachDB with UNIQUE constraints on (transaction_id)
 * and (request_id) inside a single ACID transaction boundary.
 */
public interface IdempotencyAndStateStore {
    boolean isTransactionProcessed(String transactionId);

    boolean isRequestSeen(String requestId);

    boolean isTransactionBufferedOrProcessed(String transactionId);

    void markTransactionBuffered(String transactionId);

    void unmarkTransactionBuffered(String transactionId);

    void registerRequestRecord(TransactionStateRecord record);

    void applyTransactionAtomically(
            TransactionRequest request,
            TransactionStateRecord record,
            boolean allowNegativeBalance
    );

    AccountState getOrCreateAccount(String accountId);

    Map<String, BigDecimal> snapshotAccountBalances();

    List<TransactionStateRecord> getAllRequestRecords();

    int getUniqueProcessedTransactionCount();
}
