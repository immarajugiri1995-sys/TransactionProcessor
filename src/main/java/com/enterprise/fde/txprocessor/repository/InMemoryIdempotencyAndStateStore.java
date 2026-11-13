package com.enterprise.fde.txprocessor.repository;

import com.enterprise.fde.txprocessor.domain.AccountState;
import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import com.enterprise.fde.txprocessor.domain.TransactionStateRecord;
import com.enterprise.fde.txprocessor.domain.TransactionStatus;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation of {@link IdempotencyAndStateStore} simulating
 * database unique constraints and atomic commit boundaries (Section 6.1, 6.4).
 */
public final class InMemoryIdempotencyAndStateStore implements IdempotencyAndStateStore {
    private final Set<String> processedTransactionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> bufferedTransactionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> seenRequestIds = ConcurrentHashMap.newKeySet();
    private final Map<String, AccountState> accounts = new ConcurrentHashMap<>();
    private final List<TransactionStateRecord> requestLedger = Collections.synchronizedList(new ArrayList<>());

    @Override
    public boolean isTransactionProcessed(String transactionId) {
        return transactionId != null && !transactionId.isBlank() && processedTransactionIds.contains(transactionId);
    }

    @Override
    public boolean isRequestSeen(String requestId) {
        return requestId != null && !requestId.isBlank() && seenRequestIds.contains(requestId);
    }

    @Override
    public boolean isTransactionBufferedOrProcessed(String transactionId) {
        if (transactionId == null || transactionId.isBlank()) {
            return false;
        }
        return processedTransactionIds.contains(transactionId) || bufferedTransactionIds.contains(transactionId);
    }

    @Override
    public void markTransactionBuffered(String transactionId) {
        if (transactionId != null && !transactionId.isBlank()) {
            bufferedTransactionIds.add(transactionId);
        }
    }

    @Override
    public void unmarkTransactionBuffered(String transactionId) {
        if (transactionId != null && !transactionId.isBlank()) {
            bufferedTransactionIds.remove(transactionId);
        }
    }

    @Override
    public void registerRequestRecord(TransactionStateRecord record) {
        if (record.getRequestId() != null && !record.getRequestId().isBlank()) {
            seenRequestIds.add(record.getRequestId());
        }
        requestLedger.add(record);
    }

    /**
     * Executes the balance update, idempotency key registration, and status transition to PROCESSED
     * inside a single synchronized atomic critical section so partial updates or phantom accounts cannot occur.
     */
    @Override
    public synchronized void applyTransactionAtomically(
            TransactionRequest request,
            TransactionStateRecord record,
            boolean allowNegativeBalance
    ) {
        if (processedTransactionIds.contains(request.transactionId())) {
            record.transitionTo(
                    TransactionStatus.DUPLICATE,
                    "Duplicate business transactionId detected at commit boundary: " + request.transactionId()
            );
            bufferedTransactionIds.remove(request.transactionId());
            return;
        }

        AccountState existing = accounts.get(request.accountId());
        AccountState target = existing != null ? existing : new AccountState(request.accountId());
        // Validate and apply on target before registering a newly created account in the map
        target.apply(request, allowNegativeBalance);
        if (existing == null) {
            accounts.put(request.accountId(), target);
        }

        processedTransactionIds.add(request.transactionId());
        bufferedTransactionIds.remove(request.transactionId());
        record.transitionTo(
                TransactionStatus.PROCESSED,
                "Applied " + request.transactionType() + " " + request.amount() + " " + request.normalizedCurrency()
        );
    }

    @Override
    public AccountState getOrCreateAccount(String accountId) {
        return accounts.computeIfAbsent(accountId, AccountState::new);
    }

    @Override
    public Map<String, BigDecimal> snapshotAccountBalances() {
        Map<String, BigDecimal> snapshot = new LinkedHashMap<>();
        accounts.keySet().stream().sorted().forEach(accId ->
                snapshot.put(accId, accounts.get(accId).getBalance())
        );
        return Collections.unmodifiableMap(snapshot);
    }

    @Override
    public List<TransactionStateRecord> getAllRequestRecords() {
        synchronized (requestLedger) {
            return List.copyOf(requestLedger);
        }
    }

    @Override
    public int getUniqueProcessedTransactionCount() {
        return processedTransactionIds.size();
    }
}
