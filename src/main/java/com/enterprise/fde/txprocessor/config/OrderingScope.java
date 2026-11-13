package com.enterprise.fde.txprocessor.config;

/**
 * Defines how sequence numbers are scoped for Option A (Sequence-Based Processing).
 *
 * <ul>
 *   <li>{@link #STREAM_SEQUENCE}: Monotonic sequence across the input stream/partition
 *       (matches Section 9.8 test table where sequences 1..5 span ACC-201, ACC-202, ACC-203).</li>
 *   <li>{@link #ACCOUNT_SEQUENCE}: Independent monotonic sequence (1..N) per accountId.</li>
 * </ul>
 */
public enum OrderingScope {
    STREAM_SEQUENCE,
    ACCOUNT_SEQUENCE
}
