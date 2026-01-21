package ru.practicum;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

@SpringBootApplication
@EnableDiscoveryClient
public class GatewayServerApplication {

    @Bean
    public KeyResolver userKeyResolver() {
        return exchange -> {
            // Всегда возвращаем одинаковый ключ для отключения rate limiting
            return Mono.just("test-key");
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(GatewayServerApplication.class, args);
    }
}