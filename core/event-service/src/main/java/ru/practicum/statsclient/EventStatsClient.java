package ru.practicum.statsclient;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventStatsClient {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String STATS_SERVER_URL = "http://STATS-SERVER";

    @Qualifier("simpleRestTemplate")
    private final RestTemplate restTemplate;

    @Value("${stats.client.connect-timeout:1000}")
    private int connectTimeout;

    @Value("${stats.client.read-timeout:2000}")
    private int readTimeout;

    @PostConstruct
    public void init() {
        if (restTemplate.getRequestFactory() instanceof HttpComponentsClientHttpRequestFactory) {
            HttpComponentsClientHttpRequestFactory requestFactory =
                    (HttpComponentsClientHttpRequestFactory) restTemplate.getRequestFactory();
            requestFactory.setConnectTimeout(connectTimeout);
            requestFactory.setReadTimeout(readTimeout);
            log.debug("EventStatsClient timeouts set: connect={}ms, read={}ms", connectTimeout, readTimeout);
        }
    }

    public void saveHit(EndpointHitDTO endpointHitDTO) {
        executePostRequest("/hit", endpointHitDTO, Void.class, "Failed to save hit");
    }

    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            return;
        }

        try {
            executePostRequest("/hit/batch", hits, Void.class, "Failed to save batch hits");
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

            String uris = eventIds.stream()
                    .map(id -> "/events/" + id)
                    .collect(Collectors.joining(","));

            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromHttpUrl(STATS_SERVER_URL)
                    .path("/stats")
                    .queryParam("start", FORMATTER.format(start))
                    .queryParam("end", FORMATTER.format(end))
                    .queryParam("unique", true)
                    .queryParam("uris", uris);

            ViewStatsDTO[] response = executeGetRequest(uriBuilder.build().toUri(), ViewStatsDTO[].class);

            if (response != null) {
                return processStatsResponse(eventIds, response);
            }

        } catch (Exception e) {
            log.error("Failed to get stats from stats server: {}", e.getMessage());
        }

        return createDefaultViewsMap(eventIds);
    }

    private <T> void executePostRequest(String path, Object body, Class<T> responseType, String errorMessage) {
        try {
            URI uri = UriComponentsBuilder.fromHttpUrl(STATS_SERVER_URL)
                    .path(path)
                    .build()
                    .toUri();

            HttpHeaders headers = createJsonHeaders();
            HttpEntity<Object> requestEntity = new HttpEntity<>(body, headers);

            log.debug("Sending POST request to {} with body: {}", uri, body);

            ResponseEntity<T> response = restTemplate.exchange(uri, HttpMethod.POST, requestEntity, responseType);

            log.debug("Request successful. Status: {}", response.getStatusCode());

        } catch (Exception e) {
            log.error("{}: {}", errorMessage, e.getMessage());
        }
    }

    private <T> T executeGetRequest(URI uri, Class<T> responseType) {
        try {
            HttpHeaders headers = createJsonHeaders();
            HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

            log.debug("Sending GET request to: {}", uri);

            ResponseEntity<T> response = restTemplate.exchange(uri, HttpMethod.GET, requestEntity, responseType);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody();
            }

        } catch (Exception e) {
            log.error("GET request failed for {}: {}", uri, e.getMessage());
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
        Map<Long, Long> viewsMap = Arrays.stream(stats)
                .filter(stat -> stat.getUri() != null && stat.getUri().startsWith("/events/"))
                .collect(Collectors.toMap(
                        stat -> extractEventIdFromUri(stat.getUri()),
                        ViewStatsDTO::getHits,
                        (existing, replacement) -> existing // в случае дублирования сохраняем существующее значение
                ));

        // Заполняем нулями для событий без статистики
        eventIds.forEach(eventId -> viewsMap.putIfAbsent(eventId, 0L));

        return viewsMap;
    }

    private Long extractEventIdFromUri(String uri) {
        try {
            return Long.parseLong(uri.replace("/events/", ""));
        } catch (NumberFormatException e) {
            log.warn("Invalid event ID in URI: {}", uri);
            return null;
        }
    }

    private Map<Long, Long> createDefaultViewsMap(List<Long> eventIds) {
        return eventIds.stream()
                .collect(Collectors.toMap(id -> id, id -> 0L));
    }
}