package ru.mikhalev.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.random.RandomGenerator;

@Configuration
public class AppConfig {

    @Bean
    public RandomGenerator randomGenerator() {
        return RandomGenerator.getDefault();
    }
}
