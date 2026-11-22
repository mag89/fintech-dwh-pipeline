package ru.mikhalev.kafka;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaStreamApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(KafkaStreamApplication.class, args);

        System.exit(SpringApplication.exit(ctx, () -> 0));
    }

    @Bean
    public CommandLineRunner batchRunner(StagingIngestorService ingestorService) {
        return args -> {

            for (int i = 0; i < 20; i++) {
                ingestorService.produceTransaction();
            }
          
            Thread.sleep(3000);
        };
    }
}
