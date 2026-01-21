package ru.practicum.mainservice.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import ru.practicum.mainservice.statsclient.StatsClientImpl;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class StatsServiceHealthIndicator implements HealthIndicator {

    private final StatsClientImpl statsClient;

    @Override
    public Health health() {
        try {
            // Проверяем, можем ли получить статистику за последний час
            LocalDateTime end = LocalDateTime.now();
            LocalDateTime start = end.minusHours(1);

            // Это тестовый вызов с минимальными параметрами
            statsClient.getStats(start, end, null, false);

            return Health.up()
                    .withDetail("service", "stats-service")
                    .withDetail("status", "available")
                    .withDetail("timestamp", LocalDateTime.now())
                    .build();

        } catch (Exception e) {
            log.warn("Stats service health check failed: {}", e.getMessage());

            return Health.down()
                    .withDetail("service", "stats-service")
                    .withDetail("status", "unavailable")
                    .withDetail("error", e.getMessage())
                    .withDetail("timestamp", LocalDateTime.now())
                    .build();
        }
    }
}