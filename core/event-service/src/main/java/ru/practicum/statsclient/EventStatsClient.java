package ru.practicum.statsclient;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventStatsClient {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String STATS_SERVICE_NAME = "stats-server";

    // Используем LoadBalanced RestTemplate для работы с Eureka
    @Qualifier("loadBalancedRestTemplate")
    private final RestTemplate restTemplate;

    @Value("${stats.client.connect-timeout:3000}")
    private int connectTimeout;

    @Value("${stats.client.read-timeout:5000}")
    private int readTimeout;

    @PostConstruct
    public void init() {
        // Настраиваем таймауты
        if (restTemplate.getRequestFactory() instanceof HttpComponentsClientHttpRequestFactory) {
            HttpComponentsClientHttpRequestFactory requestFactory =
                    (HttpComponentsClientHttpRequestFactory) restTemplate.getRequestFactory();
            requestFactory.setConnectTimeout(connectTimeout);
            requestFactory.setReadTimeout(readTimeout);
            log.debug("EventStatsClient timeouts set: connect={}ms, read={}ms", connectTimeout, readTimeout);
        }

        log.info("EventStatsClient initialized using service name: {}", STATS_SERVICE_NAME);
    }

    public void saveHit(EndpointHitDTO endpointHitDTO) {
        String url = String.format("http://%s/hit", STATS_SERVICE_NAME);
        executePostRequest(url, endpointHitDTO, Void.class, "Failed to save hit");
    }

    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            return;
        }

        String url = String.format("http://%s/hit/batch", STATS_SERVICE_NAME);
        try {
            executePostRequest(url, hits, Void.class, "Failed to save batch hits");
            log.debug("Batch of {} hits sent successfully", hits.size());
        } catch (Exception e) {
            log.warn("Batch save failed, saving individually: {}", e.getMessage());
            hits.forEach(this::saveHit);
        }
    }

    public Map<Long, Long> getViewsForEvents(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            LocalDateTime start = LocalDateTime.now().minusYears(10);
            LocalDateTime end = LocalDateTime.now().plusYears(10);

            List<String> uriList = eventIds.stream()
                    .map(id -> "/events/" + id)
                    .collect(Collectors.toList());

            String url = String.format("http://%s/stats", STATS_SERVICE_NAME);

            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromHttpUrl(url)
                    .queryParam("start", FORMATTER.format(start))
                    .queryParam("end", FORMATTER.format(end))
                    .queryParam("unique", true);

            // Добавляем каждый URI отдельным параметром
            for (String uri : uriList) {
                uriBuilder.queryParam("uris", uri);
            }

            String fullUrl = uriBuilder.toUriString();
            log.debug("Getting stats from: {}", fullUrl);

            ViewStatsDTO[] response = executeGetRequest(fullUrl, ViewStatsDTO[].class);

            if (response != null) {
                return processStatsResponse(eventIds, response);
            }

        } catch (Exception e) {
            log.warn("Failed to get stats from stats server: {}", e.getMessage());
            log.debug("Stack trace:", e);
        }

        // Возвращаем мапу с нулевыми просмотрами при ошибке
        return createDefaultViewsMap(eventIds);
    }

    private <T> void executePostRequest(String url, Object body, Class<T> responseType, String errorMessage) {
        try {
            HttpHeaders headers = createJsonHeaders();
            HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);

            log.debug("Sending POST request to {} with body: {}", url, body);

            ResponseEntity<T> response = restTemplate.exchange(
                    url, HttpMethod.POST, requestEntity, responseType);

            log.debug("Request successful. Status: {}", response.getStatusCode());

        } catch (Exception e) {
            log.warn("{} ({}): {}", errorMessage, url, e.getMessage());
        }
    }

    private <T> T executeGetRequest(String url, Class<T> responseType) {
        try {
            HttpHeaders headers = createJsonHeaders();
            HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

            log.debug("Sending GET request to: {}", url);

            ResponseEntity<T> response = restTemplate.exchange(
                    url, HttpMethod.GET, requestEntity, responseType);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            } else {
                log.warn("GET request to {} returned status: {}", url, response.getStatusCode());
            }

        } catch (Exception e) {
            log.warn("GET request failed for {}: {}", url, e.getMessage());
        }

        return null;
    }

    private HttpHeaders createJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        return headers;
    }

    private Map<Long, Long> processStatsResponse(List<Long> eventIds, ViewStatsDTO[] stats) {
        Map<Long, Long> viewsMap = new HashMap<>();

        if (stats != null) {
            for (ViewStatsDTO stat : stats) {
                if (stat != null && stat.getUri() != null && stat.getUri().startsWith("/events/")) {
                    try {
                        Long eventId = Long.parseLong(stat.getUri().replace("/events/", ""));
                        viewsMap.put(eventId, stat.getHits() != null ? stat.getHits() : 0L);
                    } catch (NumberFormatException e) {
                        log.warn("Invalid event ID in URI: {}", stat.getUri());
                    }
                }
            }
        }

        // Заполняем нулями для событий без статистики
        for (Long eventId : eventIds) {
            viewsMap.putIfAbsent(eventId, 0L);
        }

        log.debug("Processed stats for {} events, found {} with views", eventIds.size(), viewsMap.size());
        return viewsMap;
    }

    private Map<Long, Long> createDefaultViewsMap(List<Long> eventIds) {
        Map<Long, Long> defaultMap = new HashMap<>();
        for (Long eventId : eventIds) {
            defaultMap.put(eventId, 0L);
        }
        log.debug("Created default views map for {} events", eventIds.size());
        return defaultMap;
    }
}