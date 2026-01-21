package ru.practicum.statsclient.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Клиент для взаимодействия со статистическим сервером.
 * Отправляет данные о просмотрах и получает статистику.
 */
@Slf4j
@Component
public class StatsClient {
    private final RestClient restClient;
    private final String serverUrl;

    /**
     * Конструктор по умолчанию для Spring.
     * Используется, когда URL сервера не указан явно.
     */
    public StatsClient() {
        this("http://STATS-SERVER");
    }

    /**
     * Конструктор для создания клиента с явным указанием URL сервера.
     * @param serverUrl URL статистического сервера
     */
    public StatsClient(@Value("${stats.server.url:http://STATS-SERVER}") String serverUrl) {
        this.serverUrl = serverUrl;
        log.info("Инициализация StatsClient с URL сервера: {}", serverUrl);

        // Используем стандартный RestTemplate без HttpComponentsClientHttpRequestFactory
        RestTemplate restTemplate = new RestTemplateBuilder()
                .setConnectTimeout(Duration.ofSeconds(5))
                .setReadTimeout(Duration.ofSeconds(10))
                .build();

        this.restClient = RestClient.builder()
                .baseUrl(serverUrl)
                .build();
    }

    /**
     * Альтернативный конструктор для использования с RestTemplate и именем сервиса.
     * @param restTemplate настроенный RestTemplate
     * @param serviceName имя сервиса в Eureka
     */
    public StatsClient(RestTemplate restTemplate, String serviceName) {
        this.serverUrl = "http://" + serviceName;
        log.info("Инициализация StatsClient с именем сервиса: {}", serviceName);

        this.restClient = RestClient.builder()
                .baseUrl(this.serverUrl)
                .build();
    }

    /**
     * Сохраняет информацию о просмотре (hit).
     * @param endpointHitDTO DTO с информацией о просмотре
     */
    public void saveHit(EndpointHitDTO endpointHitDTO) {
        try {
            log.debug("Отправка hit в статистический сервер: {}", endpointHitDTO);

            ResponseEntity<Void> response = restClient.post()
                    .uri("/hit")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(endpointHitDTO)
                    .retrieve()
                    .toBodilessEntity();

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Hit успешно отправлен в статистический сервер");
            } else {
                log.warn("Не удалось отправить hit. Статус ответа: {}", response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Ошибка при отправке hit в статистический сервер: {}", e.getMessage(), e);
        }
    }

    /**
     * Сохраняет несколько просмотров (hits) пачкой.
     * В случае ошибки пачки, отправляет по одному.
     * @param hits список DTO с информацией о просмотрах
     */
    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            log.debug("Список hits пуст, пропускаем отправку");
            return;
        }

        try {
            log.debug("Отправка batch из {} hits в статистический сервер", hits.size());

            ResponseEntity<Void> response = restClient.post()
                    .uri("/hit/batch")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(hits)
                    .retrieve()
                    .toBodilessEntity();

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Batch из {} hits успешно отправлен в статистический сервер", hits.size());
            } else {
                log.warn("Не удалось отправить batch hits. Статус ответа: {}", response.getStatusCode());
            }
        } catch (Exception e) {
            log.warn("Ошибка при отправке batch hits. Отправляем hits по одному: {}", e.getMessage());

            for (EndpointHitDTO hit : hits) {
                try {
                    saveHit(hit);
                } catch (Exception ex) {
                    log.error("Не удалось отправить hit в fallback режиме: {}", ex.getMessage());
                }
            }
        }
    }

    /**
     * Получает статистику просмотров за указанный период.
     * @param start начальная дата периода
     * @param end конечная дата периода
     * @param uris список URI для фильтрации (опционально)
     * @param unique учитывать только уникальные IP
     * @return список статистических данных
     * @throws IllegalArgumentException если даты некорректны
     */
    public List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end,
                                       List<String> uris, Boolean unique) {
        validateDates(start, end);

        try {
            UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(serverUrl + "/stats")
                    .queryParam("start", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                    .queryParam("end", end.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                    .queryParam("unique", unique != null ? unique : false);

            if (uris != null && !uris.isEmpty()) {
                // Для нескольких URI передаем как повторяющийся параметр
                for (String uri : uris) {
                    uriBuilder.queryParam("uris", uri);
                }
            }

            String url = uriBuilder.toUriString();
            log.debug("Запрос статистики: {}", url);

            ViewStatsDTO[] response = restClient.get()
                    .uri(url)
                    .retrieve()
                    .body(ViewStatsDTO[].class);

            List<ViewStatsDTO> stats = response != null ? Arrays.asList(response) : Collections.emptyList();
            log.debug("Получено {} записей статистики", stats.size());
            return stats;

        } catch (Exception e) {
            log.error("Ошибка при получении статистики: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * Валидирует даты для запроса статистики.
     * @param start начальная дата
     * @param end конечная дата
     * @throws IllegalArgumentException если даты некорректны
     */
    private void validateDates(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("Даты не могут быть null");
        }
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Начальная дата не может быть позже конечной даты");
        }
    }

    /**
     * Получает URL сервера статистики.
     * @return URL сервера
     */
    public String getServerUrl() {
        return serverUrl;
    }
}