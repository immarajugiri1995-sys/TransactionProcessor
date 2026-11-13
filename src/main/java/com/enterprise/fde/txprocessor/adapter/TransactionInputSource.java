package com.enterprise.fde.txprocessor.adapter;

import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import java.util.List;

/**
 * Replaceable input source abstraction (L2-FR-01, L2-FR-10).
 * Can be backed by an in-memory collection, local JSON file, database outbox poller,
 * or enterprise message queue consumer (Kafka, RabbitMQ, AWS SQS).
 */
@FunctionalInterface
public interface TransactionInputSource {
    List<TransactionRequest> fetchBatch();
}
