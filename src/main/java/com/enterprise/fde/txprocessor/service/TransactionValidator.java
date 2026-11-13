package com.enterprise.fde.txprocessor.service;

import com.enterprise.fde.txprocessor.domain.TransactionRequest;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Validates mandatory fields and business invariants prior to processing (L2-FR-02, Section 6.6).
 */
public final class TransactionValidator {

    public Optional<String> validate(TransactionRequest request) {
        if (request == null) {
            return Optional.of("Malformed record: request payload is null");
        }
        List<String> errors = new ArrayList<>();

        if (isBlank(request.transactionId())) {
            errors.add("transactionId is mandatory");
        }
        if (isBlank(request.requestId())) {
            errors.add("requestId is mandatory");
        }
        if (isBlank(request.accountId())) {
            errors.add("accountId is mandatory");
        }
        if (request.transactionType() == null) {
            errors.add("transactionType must be one of CREDIT, DEBIT, REVERSAL");
        }
        if (request.amount() == null) {
            errors.add("amount is mandatory");
        } else if (request.amount().compareTo(BigDecimal.ZERO) <= 0) {
            errors.add("amount must be strictly positive (> 0), got " + request.amount());
        }
        if (request.sequenceNumber() <= 0) {
            errors.add("sequenceNumber must be >= 1, got " + request.sequenceNumber());
        }
        String currency = request.normalizedCurrency();
        if (currency.length() != 3) {
            errors.add("currency must be a 3-letter ISO code");
        }

        if (errors.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of("Validation failed: " + String.join("; ", errors));
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
