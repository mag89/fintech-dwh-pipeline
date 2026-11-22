package ru.mikhalev.kafka;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.List;
import java.util.random.RandomGenerator;
import java.util.UUID;

@Service
public class StagingIngestorService {

    private static final Logger log = LoggerFactory.getLogger(StagingIngestorService.class);
    private static final String TOPIC = "raw-transactions";
    private static final List<String> CATEGORIES = List.of("SUPERMARKETS", "IT_SERVICES", "TRANSPORT", "RESTAURANTS");

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final RandomGenerator randomGenerator;
	
	public StagingIngestorService(KafkaTemplate<String, TransactionEvent> kafkaTemplate, JdbcTemplate jdbcTemplate, RandomGenerator randomGenerator) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.randomGenerator = randomGenerator;
    }

    @PostConstruct
    public void initStagingTable() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS raw_transactions (
                tx_id VARCHAR(36) PRIMARY KEY,
                user_id BIGINT NOT NULL,
                merchant_category VARCHAR(50) NOT NULL,
                amount NUMERIC(15, 2) NOT NULL,
                is_suspicious BOOLEAN NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """);
        log.info("Staging-таблица raw_transactions готова к приёму потока");
    }

    @Scheduled(fixedRate = 3000)
    public void produceTransaction() {
        String category = CATEGORIES.get(randomGenerator.nextInt(CATEGORIES.size()));
        BigDecimal amount = BigDecimal.valueOf(100 + (150000 - 100) * randomGenerator.nextDouble())
                .setScale(2, RoundingMode.HALF_UP);

        TransactionEvent event = new TransactionEvent(
                UUID.randomUUID().toString(),
                (long) (1000 + randomGenerator.nextInt(30)),
                category,
                amount,
                LocalDateTime.now()
        );

        kafkaTemplate.send(TOPIC, event.txId(), event);
        log.info("[KAFKA PRODUCER] -> txId={}, user={}, cat={}, amount={}",
                event.txId(), event.userId(), category, amount);
    }

    @KafkaListener(topics = TOPIC, groupId = "dwh-staging-group")
    public void consumeToStaging(TransactionEvent event) {
        boolean isSuspicious = event.amount().compareTo(new BigDecimal("100000.00")) > 0;

        jdbcTemplate.update("""
            INSERT INTO raw_transactions (tx_id, user_id, merchant_category, amount, is_suspicious, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (tx_id) DO NOTHING
            """,
                event.txId(), event.userId(), event.merchantCategory(), event.amount(), isSuspicious, event.createdAt()
        );

        log.info("[KAFKA CONSUMER -> STAGING] Сохранено txId={}, suspicious={}", event.txId(), isSuspicious);
    }
}