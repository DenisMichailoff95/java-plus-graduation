package ru.practicum.statsclient;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventStatsClient {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
        try {
            String url = "http://STATS-SERVER/hit";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<EndpointHitDTO> requestEntity = new HttpEntity<>(endpointHitDTO, headers);

            log.debug("Sending hit to stats server: {}", endpointHitDTO);

            ResponseEntity<Void> response = restTemplate.exchange(
                    url, HttpMethod.POST, requestEntity, Void.class);

            log.debug("Hit successfully sent. Status: {}", response.getStatusCode());

        } catch (Exception e) {
            log.error("Failed to save hit to stats server: {}", e.getMessage());
        }
    }

    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            return;
        }

        try {
            String url = "http://STATS-SERVER/hit/batch";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<List<EndpointHitDTO>> requestEntity = new HttpEntity<>(hits, headers);

            log.debug("Sending batch of {} hits to stats server", hits.size());

            ResponseEntity<Void> response = restTemplate.exchange(
                    url, HttpMethod.POST, requestEntity, Void.class);

            log.debug("Batch hits sent successfully. Status: {}", response.getStatusCode());

        } catch (Exception e) {
            log.warn("Batch save failed: {}", e.getMessage());

            for (EndpointHitDTO hit : hits) {
                saveHit(hit);
            }
        }
    }

    public Map<Long, Long> getViewsForEvents(List<Long> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            LocalDateTime start = LocalDateTime.now().minusYears(10);
            LocalDateTime end = LocalDateTime.now().plusYears(10);

            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromHttpUrl("http://STATS-SERVER")
                    .path("/stats")
                    .queryParam("start", FORMATTER.format(start))
                    .queryParam("end", FORMATTER.format(end))
                    .queryParam("unique", true);

            eventIds.forEach(eventId ->
                    uriBuilder.queryParam("uris", "/events/" + eventId));

            URI uri = uriBuilder.build().toUri();
            log.debug("Getting stats from: {}", uri);

            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

            ResponseEntity<ViewStatsDTO[]> response = restTemplate.exchange(
                    uri, HttpMethod.GET, requestEntity, ViewStatsDTO[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<Long, Long> viewsMap = new HashMap<>();
                for (ViewStatsDTO stat : response.getBody()) {
                    try {
                        Long eventId = Long.parseLong(stat.getUri().replace("/events/", ""));
                        viewsMap.put(eventId, stat.getHits());
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }

                for (Long eventId : eventIds) {
                    viewsMap.putIfAbsent(eventId, 0L);
                }

                return viewsMap;
            }

        } catch (Exception e) {
            log.error("Failed to get stats from stats server: {}", e.getMessage());
        }

        return eventIds.stream().collect(Collectors.toMap(id -> id, id -> 0L));
    }
}