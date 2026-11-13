package com.enterprise.fde.txprocessor.domain;

/**
 * Complete transaction lifecycle states (Section 5 & L2-FR-04).
 */
public enum TransactionStatus {
    RECEIVED,
    PENDING,
    PROCESSING,
    RETRY_PENDING,
    PROCESSED,
    DUPLICATE,
    FAILED
}
