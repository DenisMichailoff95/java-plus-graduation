package ru.practicum.mainservice.statsclient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.http.*;
import org.springframework.retry.support.RetryTemplate;
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
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final DiscoveryClient discoveryClient;
    private final RetryTemplate retryTemplate;
    private final @Qualifier("simpleRestTemplate") RestTemplate simpleRestTemplate;

    public void saveHit(EndpointHitDTO endpointHitDTO) {
        try {
            URI uri = buildUri("/hit");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<EndpointHitDTO> requestEntity = new HttpEntity<>(endpointHitDTO, headers);

            log.debug("Sending hit to stats server: {}", endpointHitDTO);
            ResponseEntity<Void> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.POST, requestEntity, Void.class);

            log.debug("Hit successfully sent to stats server. Status: {}", response.getStatusCode());

        } catch (Exception e) {
            log.error("Failed to save hit to stats server: {}", e.getMessage(), e);
        }
    }

    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            return;
        }

        try {
            URI uri = buildUri("/hit/batch");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<List<EndpointHitDTO>> requestEntity = new HttpEntity<>(hits, headers);

            log.debug("Sending batch of {} hits to stats server", hits.size());
            simpleRestTemplate.exchange(uri, HttpMethod.POST, requestEntity, Void.class);
            log.debug("Batch hits successfully sent to stats server");

        } catch (Exception e) {
            log.warn("Batch save failed, falling back to single saves: {}", e.getMessage());

            for (EndpointHitDTO hit : hits) {
                try {
                    saveHit(hit);
                } catch (Exception ex) {
                    log.error("Failed to save hit in fallback mode: {}", ex.getMessage());
                }
            }
        }
    }

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
            log.error("Failed to get stats from stats server: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    private URI buildStatsUri(LocalDateTime start, LocalDateTime end,
                              List<String> uris, Boolean unique) {
        return retryTemplate.execute(context -> {
            ServiceInstance instance = getStatsServiceInstance();

            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromHttpUrl("http://" + instance.getHost() + ":" + instance.getPort())
                    .path("/stats")
                    .queryParam("start", FORMATTER.format(start))
                    .queryParam("end", FORMATTER.format(end))
                    .queryParam("unique", unique);

            if (uris != null && !uris.isEmpty()) {
                for (String uri : uris) {
                    uriBuilder.queryParam("uris", uri);
                }
            }

            return uriBuilder.build().toUri();
        });
    }

    private URI buildUri(String path) {
        return retryTemplate.execute(context -> {
            ServiceInstance instance = getStatsServiceInstance();
            return URI.create("http://" + instance.getHost() + ":" + instance.getPort() + path);
        });
    }

    private ServiceInstance getStatsServiceInstance() {
        List<ServiceInstance> instances = discoveryClient.getInstances(STATS_SERVICE_ID);

        if (instances == null || instances.isEmpty()) {
            String errorMsg = "Stats service (" + STATS_SERVICE_ID + ") not found in Eureka registry";
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        ServiceInstance instance = instances.get(0);
        log.debug("Found stats service instance: {}:{}", instance.getHost(), instance.getPort());

        return instance;
    }

    private void validateDates(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("Dates must not be null");
        }
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Start date must be before end date");
        }
    }
}