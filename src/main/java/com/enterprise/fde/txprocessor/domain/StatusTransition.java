package com.enterprise.fde.txprocessor.domain;

import java.time.Instant;

/**
 * Immutable audit trail entry capturing a single state transition (L2-FR-03, L2-FR-04).
 */
public record StatusTransition(
        TransactionStatus fromStatus,
        TransactionStatus toStatus,
        String reason,
        Instant timestamp
) {
}
