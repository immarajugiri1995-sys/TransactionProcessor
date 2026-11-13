package com.enterprise.fde.txprocessor.adapter;

import com.enterprise.fde.txprocessor.domain.TransactionRequest;
import com.enterprise.fde.txprocessor.domain.TransactionType;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads a JSON array of transaction requests from a local file path (L2-FR-01).
 * Uses lightweight zero-external-dependency parsing so it can run both with Maven/Jackson
 * or standalone via standard JDK 21 without external classpath setup.
 */
public final class JsonFileTransactionInputSource implements TransactionInputSource {
    private static final Pattern OBJECT_PATTERN = Pattern.compile("\\{([^\\{\\}]*)\\}");
    private static final Pattern KV_PATTERN = Pattern.compile("\"([^\"]+)\"\\s*:\\s*(\"[^\"]*\"|[-0-9.]+|true|false|null)");

    private final Path filePath;

    public JsonFileTransactionInputSource(Path filePath) {
        this.filePath = filePath;
    }

    @Override
    public List<TransactionRequest> fetchBatch() {
        try {
            String rawJson = Files.readString(filePath, StandardCharsets.UTF_8);
            return parseJsonArray(rawJson);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read transaction input file: " + filePath, e);
        }
    }

    public static List<TransactionRequest> parseJsonArray(String rawJson) {
        List<TransactionRequest> results = new ArrayList<>();
        Matcher objMatcher = OBJECT_PATTERN.matcher(rawJson);
        while (objMatcher.find()) {
            String body = objMatcher.group(1);
            String transactionId = null;
            String requestId = null;
            String accountId = null;
            TransactionType type = null;
            BigDecimal amount = null;
            String currency = "USD";
            long sequence = 0L;
            Instant eventTime = Instant.now();

            Matcher kvMatcher = KV_PATTERN.matcher(body);
            while (kvMatcher.find()) {
                String key = kvMatcher.group(1).trim();
                String rawVal = kvMatcher.group(2).trim();
                String val = rawVal.startsWith("\"") && rawVal.endsWith("\"")
                        ? rawVal.substring(1, rawVal.length() - 1)
                        : rawVal;
                if ("null".equals(rawVal)) {
                    val = null;
                }
                switch (key) {
                    case "transactionId" -> transactionId = val;
                    case "requestId", "idempotencyKey" -> requestId = val;
                    case "accountId" -> accountId = val;
                    case "type", "transactionType" -> {
                        if (val != null && !val.isBlank()) {
                            try {
                                type = TransactionType.valueOf(val.trim().toUpperCase());
                            } catch (IllegalArgumentException ignored) {
                                type = null;
                            }
                        }
                    }
                    case "amount" -> {
                        if (val != null) {
                            try {
                                amount = new BigDecimal(val);
                            } catch (NumberFormatException ignored) {
                                amount = null;
                            }
                        }
                    }
                    case "currency" -> currency = val;
                    case "sequence", "sequenceNumber" -> {
                        if (val != null) {
                            try {
                                sequence = Long.parseLong(val);
                            } catch (NumberFormatException ignored) {
                                sequence = -1L;
                            }
                        }
                    }
                    case "eventTime" -> {
                        if (val != null) {
                            try {
                                eventTime = Instant.parse(val);
                            } catch (Exception ignored) {
                                eventTime = Instant.now();
                            }
                        }
                    }
                    default -> {
                    }
                }
            }
            results.add(new TransactionRequest(
                    transactionId,
                    requestId,
                    accountId,
                    type,
                    amount,
                    currency,
                    sequence,
                    eventTime
            ));
        }
        return results;
    }
}
