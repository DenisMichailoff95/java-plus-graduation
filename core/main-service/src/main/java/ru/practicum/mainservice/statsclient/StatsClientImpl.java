package ru.practicum.mainservice.statsclient;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class StatsClientImpl {

    private static final String STATS_SERVICE_BASE_URL = "http://stats-server";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final RestTemplate restTemplate;

    @Autowired
    public StatsClientImpl(@Qualifier("simpleRestTemplate") RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public void saveHit(EndpointHitDTO endpointHitDTO) {
        try {
            String url = STATS_SERVICE_BASE_URL + "/hit";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<EndpointHitDTO> requestEntity = new HttpEntity<>(endpointHitDTO, headers);

            ResponseEntity<Void> response = restTemplate.exchange(
                    url, HttpMethod.POST, requestEntity, Void.class);

            log.debug("Hit sent to stats server: {}", endpointHitDTO);
        } catch (Exception e) {
            log.warn("Failed to send hit to stats server: {}", e.getMessage());
        }
    }

    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            return;
        }

        for (EndpointHitDTO hit : hits) {
            saveHit(hit);
        }
    }

    @Cacheable(value = "eventStats", key = "{#start, #end, #uris, #unique}")
    public List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end,
                                       List<String> uris, Boolean unique) {
        try {
            UriComponentsBuilder uriBuilder = UriComponentsBuilder
                    .fromHttpUrl(STATS_SERVICE_BASE_URL + "/stats")
                    .queryParam("start", FORMATTER.format(start))
                    .queryParam("end", FORMATTER.format(end))
                    .queryParam("unique", unique);

            if (uris != null && !uris.isEmpty()) {
                uriBuilder.queryParam("uris", String.join(",", uris));
            }

            String url = uriBuilder.toUriString();

            ResponseEntity<ViewStatsDTO[]> response = restTemplate.getForEntity(
                    url, ViewStatsDTO[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return Arrays.asList(response.getBody());
            }
        } catch (Exception e) {
            log.warn("Failed to get stats from stats server: {}", e.getMessage());
        }

        return Collections.emptyList();
    }
}