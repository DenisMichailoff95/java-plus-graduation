package ru.practicum.statsclient.client;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public abstract class StatsClient {
    private final RestClient restClient;
    private final String baseUrl;

    public StatsClient(String serverUrl) {
        this.baseUrl = serverUrl;
        this.restClient = RestClient.builder()
                .baseUrl(serverUrl)
                .build();
    }

    public StatsClient(RestTemplate restTemplate, String serviceName) {
        this.baseUrl = "http://" + serviceName;
        this.restClient = RestClient.builder()
                .baseUrl(this.baseUrl)
                .build();
    }

    public void saveHit(EndpointHitDTO endpointHitDTO) {
        try {
            restClient.post()
                    .uri("/hit")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(endpointHitDTO)
                    .retrieve()
                    .toBodilessEntity();
        } catch (Exception e) {
            System.err.println("Failed to save hit: " + e.getMessage());
        }
    }

    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            return;
        }

        try {
            restClient.post()
                    .uri("/hit/batch")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(hits)
                    .retrieve()
                    .toBodilessEntity();
        } catch (Exception e) {
            for (EndpointHitDTO hit : hits) {
                saveHit(hit);
            }
        }
    }

    public List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end, List<String> uris, Boolean unique) {
        validateDates(start, end);

        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(baseUrl + "/stats")
                .queryParam("start", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(start))
                .queryParam("end", DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(end))
                .queryParam("unique", unique);

        if (uris != null && !uris.isEmpty()) {
            uriBuilder.queryParam("uris", String.join(",", uris));
        }

        String url = uriBuilder.toUriString();

        ViewStatsDTO[] response = restClient.get()
                .uri(url)
                .retrieve()
                .body(ViewStatsDTO[].class);

        return response != null ? Arrays.asList(response) : List.of();
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