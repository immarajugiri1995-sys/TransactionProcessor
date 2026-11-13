package com.enterprise.fde.txprocessor.adapter;

import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import java.util.List;

/**
 * In-memory input source for unit testing and embedded simulations.
 */
public final class InMemoryTransactionInputSource implements TransactionInputSource {
    private final List<TransactionRequest> requests;

    public InMemoryTransactionInputSource(List<TransactionRequest> requests) {
        this.requests = requests == null ? List.of() : List.copyOf(requests);
    }

    @Override
    public List<TransactionRequest> fetchBatch() {
        return requests;
    }
}
