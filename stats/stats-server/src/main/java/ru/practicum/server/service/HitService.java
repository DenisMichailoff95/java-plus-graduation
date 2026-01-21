package ru.practicum.server.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;
import ru.practicum.server.model.Hit;
import ru.practicum.server.repository.HitRepository;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class HitService {

    private final HitRepository hitRepository;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int MAX_URIS_PER_QUERY = 100;
    private static final int MAX_DATE_RANGE_DAYS = 365;

    @Async
    @Transactional
    @CacheEvict(value = "stats", allEntries = true)
    public CompletableFuture<Void> createHitAsync(EndpointHitDTO endpointHitDTO) {
        return CompletableFuture.runAsync(() -> {
            try {
                createHit(endpointHitDTO);
            } catch (Exception e) {
                log.error("Failed to create hit asynchronously: {}", e.getMessage(), e);
            }
        });
    }

    @Transactional
    public void createHit(EndpointHitDTO endpointHitDTO) {
        try {
            validateHit(endpointHitDTO);

            Hit hit = Hit.builder()
                    .app(endpointHitDTO.getApp().trim())
                    .uri(endpointHitDTO.getUri().trim())
                    .ip(endpointHitDTO.getIp().trim())
                    .timestamp(parseTimestamp(endpointHitDTO.getTimestamp()))
                    .build();

            hitRepository.save(hit);
            log.debug("Hit saved: {}", hit);

        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate hit detected: {}", endpointHitDTO);
            // Игнорируем дубликаты, так как у нас есть unique constraint
        } catch (Exception e) {
            log.error("Failed to save hit: {}", e.getMessage(), e);
            throw e;
        }
    }

    @Transactional(readOnly = true)
    @Cacheable(value = "stats", key = "{#start, #end, #uris, #unique}")
    public List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end,
                                       List<String> uris, Boolean unique) {
        log.debug("Getting stats: start={}, end={}, uris={}, unique={}", start, end, uris, unique);

        validateDateRange(start, end);
        validateUris(uris);

        List<ViewStatsDTO> stats;

        if (Boolean.TRUE.equals(unique)) {
            stats = hitRepository.getUniqueStats(start, end, uris);
        } else {
            stats = hitRepository.getStats(start, end, uris);
        }

        log.debug("Returning {} stats records", stats.size());
        return stats;
    }

    @Transactional(readOnly = true)
    public Long getTotalHits(LocalDateTime start, LocalDateTime end) {
        validateDateRange(start, end);

        return hitRepository.countByTimestampBetween(start, end);
    }

    @Transactional(readOnly = true)
    public Long getUniqueHits(LocalDateTime start, LocalDateTime end) {
        validateDateRange(start, end);

        return hitRepository.countDistinctIpByTimestampBetween(start, end);
    }

    @Transactional
    public void cleanupOldHits(int daysToKeep) {
        if (daysToKeep < 1) {
            throw new IllegalArgumentException("Days to keep must be at least 1");
        }

        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(daysToKeep);
        long deletedCount = hitRepository.deleteByTimestampBefore(cutoffDate);

        log.info("Cleaned up {} old hits older than {}", deletedCount, cutoffDate);
    }

    private void validateHit(EndpointHitDTO endpointHitDTO) {
        if (endpointHitDTO == null) {
            throw new IllegalArgumentException("Hit DTO cannot be null");
        }

        if (endpointHitDTO.getApp() == null || endpointHitDTO.getApp().trim().isEmpty()) {
            throw new IllegalArgumentException("App name cannot be empty");
        }

        if (endpointHitDTO.getUri() == null || endpointHitDTO.getUri().trim().isEmpty()) {
            throw new IllegalArgumentException("URI cannot be empty");
        }

        if (endpointHitDTO.getIp() == null || endpointHitDTO.getIp().trim().isEmpty()) {
            throw new IllegalArgumentException("IP address cannot be empty");
        }

        if (endpointHitDTO.getTimestamp() == null || endpointHitDTO.getTimestamp().trim().isEmpty()) {
            throw new IllegalArgumentException("Timestamp cannot be empty");
        }

        // Проверка длины
        if (endpointHitDTO.getApp().length() > 255) {
            throw new IllegalArgumentException("App name too long");
        }

        if (endpointHitDTO.getUri().length() > 512) {
            throw new IllegalArgumentException("URI too long");
        }

        if (endpointHitDTO.getIp().length() > 45) {
            throw new IllegalArgumentException("IP address too long");
        }
    }

    private LocalDateTime parseTimestamp(String timestampStr) {
        try {
            return LocalDateTime.parse(timestampStr, FORMATTER);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid timestamp format. Use 'yyyy-MM-dd HH:mm:ss'", e);
        }
    }

    private void validateDateRange(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("Start and end dates cannot be null");
        }

        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Start date cannot be after end date");
        }

        if (start.isAfter(LocalDateTime.now())) {
            throw new IllegalArgumentException("Start date cannot be in the future");
        }

        // Проверяем, что диапазон не слишком большой
        if (start.plusDays(MAX_DATE_RANGE_DAYS).isBefore(end)) {
            throw new IllegalArgumentException(
                    String.format("Date range cannot exceed %d days", MAX_DATE_RANGE_DAYS)
            );
        }
    }

    private void validateUris(List<String> uris) {
        if (uris != null && uris.size() > MAX_URIS_PER_QUERY) {
            throw new IllegalArgumentException(
                    String.format("Too many URIs in query. Maximum is %d", MAX_URIS_PER_QUERY)
            );
        }

        if (uris != null) {
            for (String uri : uris) {
                if (uri.length() > 512) {
                    throw new IllegalArgumentException("URI too long: " + uri);
                }
            }
        }
    }
}