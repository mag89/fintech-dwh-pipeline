package ru.mikhalev.kafka;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TransactionEvent(
        String txId,
        Long userId,
        String merchantCategory,
        BigDecimal amount,
        LocalDateTime createdAt
) {}