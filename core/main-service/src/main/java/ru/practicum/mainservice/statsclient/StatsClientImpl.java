package ru.practicum.mainservice.statsclient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.http.*;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class StatsClientImpl {

    private static final String STATS_SERVICE_ID = "STATS-SERVER";
    private static final String STATS_SERVICE_NAME = "stats-server";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private final DiscoveryClient discoveryClient;
    private final @Qualifier("simpleRestTemplate") RestTemplate simpleRestTemplate;

    @Retryable(
            retryFor = {Exception.class},
            maxAttempts = MAX_RETRY_ATTEMPTS,
            backoff = @Backoff(delay = RETRY_DELAY_MS)
    )
    public void saveHit(EndpointHitDTO endpointHitDTO) {
        try {
            URI uri = buildUri("/hit");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

            HttpEntity<EndpointHitDTO> requestEntity = new HttpEntity<>(endpointHitDTO, headers);

            log.debug("Sending hit to stats server: {}", endpointHitDTO);

            ResponseEntity<Void> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.POST, requestEntity, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.trace("Hit successfully sent to stats server");
            } else {
                log.warn("Failed to save hit: HTTP {}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("Failed to save hit to stats server after {} attempts: {}",
                    MAX_RETRY_ATTEMPTS, e.getMessage());
            throw e; // Re-throw for retry mechanism
        }
    }

    @Retryable(
            retryFor = {Exception.class},
            maxAttempts = MAX_RETRY_ATTEMPTS,
            backoff = @Backoff(delay = RETRY_DELAY_MS)
    )
    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            log.debug("No hits to save");
            return;
        }

        try {
            URI uri = buildUri("/hit/batch");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

            HttpEntity<List<EndpointHitDTO>> requestEntity = new HttpEntity<>(hits, headers);

            log.debug("Sending batch of {} hits to stats server", hits.size());

            ResponseEntity<Void> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.POST, requestEntity, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Batch hits successfully sent to stats server");
            } else {
                log.warn("Failed to save batch hits: HTTP {}", response.getStatusCode());
                fallbackToSingleSaves(hits);
            }

        } catch (Exception e) {
            log.warn("Batch save failed, falling back to single saves: {}", e.getMessage());
            fallbackToSingleSaves(hits);
        }
    }

    @Cacheable(value = "eventStats", key = "{#start, #end, #uris, #unique}")
    @Retryable(
            retryFor = {Exception.class},
            maxAttempts = MAX_RETRY_ATTEMPTS,
            backoff = @Backoff(delay = RETRY_DELAY_MS)
    )
    public List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end,
                                       List<String> uris, Boolean unique) {
        validateDates(start, end);

        try {
            URI uri = buildStatsUri(start, end, uris, unique);

            log.debug("Getting stats from stats server: start={}, end={}, uris={}, unique={}",
                    start, end, uris, unique);

            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

            ResponseEntity<ViewStatsDTO[]> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.GET, requestEntity, ViewStatsDTO[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<ViewStatsDTO> stats = Arrays.asList(response.getBody());
                log.debug("Received {} stats records from stats server", stats.size());
                return stats;
            } else {
                log.warn("No stats returned from stats server. Status: {}", response.getStatusCode());
                return Collections.emptyList();
            }

        } catch (Exception e) {
            log.error("Failed to get stats from stats server after {} attempts: {}",
                    MAX_RETRY_ATTEMPTS, e.getMessage());
            return Collections.emptyList();
        }
    }

    private URI buildStatsUri(LocalDateTime start, LocalDateTime end,
                              List<String> uris, Boolean unique) {
        ServiceInstance instance = getStatsServiceInstance();

        UriComponentsBuilder uriBuilder = UriComponentsBuilder
                .fromHttpUrl("http://" + instance.getHost() + ":" + instance.getPort())
                .path("/stats")
                .queryParam("start", FORMATTER.format(start))
                .queryParam("end", FORMATTER.format(end))
                .queryParam("unique", unique);

        if (uris != null && !uris.isEmpty()) {
            uriBuilder.queryParam("uris", String.join(",", uris));
        }

        return uriBuilder.build().toUri();
    }

    private URI buildUri(String path) {
        ServiceInstance instance = getStatsServiceInstance();
        return URI.create("http://" + instance.getHost() + ":" + instance.getPort() + path);
    }

    private ServiceInstance getStatsServiceInstance() {
        List<ServiceInstance> instances = discoveryClient.getInstances(STATS_SERVICE_ID);

        if (instances == null || instances.isEmpty()) {
            log.error("Stats service ({}) not found in Eureka registry", STATS_SERVICE_ID);
            throw new IllegalStateException("Stats service not available");
        }

        // Simple round-robin selection (can be improved)
        ServiceInstance instance = instances.get(0);
        log.debug("Selected stats service instance: {}:{}", instance.getHost(), instance.getPort());

        return instance;
    }

    private void validateDates(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("Start and end dates must not be null");
        }
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Start date must be before end date");
        }
        if (start.isAfter(LocalDateTime.now())) {
            log.warn("Start date is in the future: {}", start);
        }
    }

    private void fallbackToSingleSaves(List<EndpointHitDTO> hits) {
        log.info("Falling back to single saves for {} hits", hits.size());

        int successCount = 0;
        int failureCount = 0;

        for (EndpointHitDTO hit : hits) {
            try {
                saveHit(hit);
                successCount++;
            } catch (Exception e) {
                log.error("Failed to save hit in fallback mode: {}", e.getMessage());
                failureCount++;
            }
        }

        log.info("Fallback complete: {} successful, {} failed", successCount, failureCount);
    }
}