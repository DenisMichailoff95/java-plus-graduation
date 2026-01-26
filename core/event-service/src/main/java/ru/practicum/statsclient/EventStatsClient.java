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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventStatsClient {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String STATS_SERVICE_NAME = "stats-server";
    private static final String APP_NAME = "ewm-main-service";

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

    public void saveHit(String uri, String ip) {
        EndpointHitDTO hit = createHit(uri, ip);
        saveHit(hit);
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
            LocalDateTime start = LocalDateTime.now().minusYears(1); // Берем статистику за последний год
            LocalDateTime end = LocalDateTime.now().plusHours(1); // Добавляем час на всякий случай

            // Создаем список URI для событий
            List<String> uris = new ArrayList<>();
            for (Long eventId : eventIds) {
                uris.add("/events/" + eventId);
            }

            String url = String.format("http://%s/stats", STATS_SERVICE_NAME);

            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromHttpUrl(url)
                    .queryParam("start", FORMATTER.format(start))
                    .queryParam("end", FORMATTER.format(end))
                    .queryParam("unique", false); // false = все просмотры, true = уникальные по IP

            // Добавляем URI как отдельные параметры
            for (String uri : uris) {
                uriBuilder.queryParam("uris", uri);
            }

            String fullUrl = uriBuilder.toUriString();
            log.debug("Getting stats from: {}", fullUrl);

            ResponseEntity<ViewStatsDTO[]> response = restTemplate.exchange(
                    fullUrl, HttpMethod.GET, new HttpEntity<>(createJsonHeaders()), ViewStatsDTO[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return processStatsResponse(eventIds, response.getBody());
            } else {
                log.warn("Stats request returned status: {}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.warn("Failed to get stats from stats server: {}", e.getMessage());
            log.debug("Stack trace:", e);
        }

        // Возвращаем мапу с нулевыми просмотрами при ошибке
        return createDefaultViewsMap(eventIds);
    }

    public Long getViewsForEvent(Long eventId) {
        Map<Long, Long> views = getViewsForEvents(List.of(eventId));
        return views.getOrDefault(eventId, 0L);
    }

    private EndpointHitDTO createHit(String uri, String ip) {
        return EndpointHitDTO.builder()
                .app(APP_NAME)
                .uri(uri)
                .ip(ip)
                .timestamp(LocalDateTime.now().format(FORMATTER))
                .build();
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