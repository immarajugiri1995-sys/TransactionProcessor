package com.enterprise.fde.txprocessor.config;

/**
 * Immutable runtime configuration for the transaction processing service.
 *
 * @param maxRetries                     maximum retry attempts for transient failures before marking FAILED
 * @param orderingScope                  whether sequence ordering is tracked per stream/partition or per account
 * @param advanceSequenceOnInvalidRecord whether a malformed record with a valid sequence number advances the expected sequence cursor
 * @param allowNegativeAccountBalance    whether DEBIT transactions may bring an account below 0.00
 */
public record ProcessorConfig(
        int maxRetries,
        OrderingScope orderingScope,
        boolean advanceSequenceOnInvalidRecord,
        boolean allowNegativeAccountBalance
) {
    public ProcessorConfig {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0");
        }
        if (orderingScope == null) {
            orderingScope = OrderingScope.STREAM_SEQUENCE;
        }
    }

    public static ProcessorConfig defaultL2Config() {
        return new ProcessorConfig(
                3,
                OrderingScope.STREAM_SEQUENCE,
                true,
                false
        );
    }

    public static ProcessorConfig perAccountConfig(int maxRetries) {
        return new ProcessorConfig(
                maxRetries,
                OrderingScope.ACCOUNT_SEQUENCE,
                true,
                false
        );
    }
}
