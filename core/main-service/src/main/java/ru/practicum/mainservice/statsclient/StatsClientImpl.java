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
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class StatsClientImpl {

    private static final String STATS_SERVICE_ID = "STATS-SERVER";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String APP_NAME = "ewm-main-service";
    private static final int MAX_BATCH_SIZE = 50;
    private static final int MAX_URIS_PER_REQUEST = 100;

    private final DiscoveryClient discoveryClient;
    private final RetryTemplate retryTemplate;
    private final @Qualifier("simpleRestTemplate") RestTemplate simpleRestTemplate;

    private final Random random = new Random();
    private volatile ServiceInstance cachedInstance;
    private volatile long cacheTimestamp = 0;
    private static final long CACHE_TTL_MS = 60000; // 1 minute cache

    /**
     * Сохраняет один хит в статистику
     * @param endpointHitDTO DTO с информацией о хите
     */
    public void saveHit(EndpointHitDTO endpointHitDTO) {
        if (endpointHitDTO == null) {
            log.warn("Attempted to save null hit");
            return;
        }

        try {
            URI uri = buildUri("/hit");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

            HttpEntity<EndpointHitDTO> requestEntity = new HttpEntity<>(endpointHitDTO, headers);

            log.debug("Отправка хита в статистический сервер: {}", endpointHitDTO);

            ResponseEntity<Void> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.POST, requestEntity, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Хит успешно отправлен в статистический сервер. Статус: {}", response.getStatusCode());
            } else {
                log.warn("Не удалось отправить хит. Статус ответа: {}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("Ошибка при отправке хита в статистический сервер. Hit: {}. Ошибка: {}",
                    endpointHitDTO, e.getMessage(), e);
            // Не пробрасываем исключение дальше, чтобы не ломать основной функционал
        }
    }

    /**
     * Сохраняет несколько хитов пачкой
     * @param hits список хитов для сохранения
     */
    public void saveHits(List<EndpointHitDTO> hits) {
        if (hits == null || hits.isEmpty()) {
            log.debug("Список хитов пуст, пропускаем отправку");
            return;
        }

        // Защита от слишком больших батчей
        if (hits.size() > MAX_BATCH_SIZE) {
            log.warn("Размер батча ({}) превышает максимальный ({}). Разбиваем на части",
                    hits.size(), MAX_BATCH_SIZE);
            saveHitsInBatches(hits);
            return;
        }

        try {
            URI uri = buildUri("/hit");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));

            HttpEntity<List<EndpointHitDTO>> requestEntity = new HttpEntity<>(hits, headers);

            log.debug("Отправка батча из {} хитов в статистический сервер", hits.size());

            ResponseEntity<Void> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.POST, requestEntity, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("Батч из {} хитов успешно отправлен. Статус: {}",
                        hits.size(), response.getStatusCode());
            } else {
                log.warn("Не удалось отправить батч хитов. Статус: {}. Размер батча: {}",
                        response.getStatusCode(), hits.size());
                saveHitsIndividually(hits); // Fallback
            }

        } catch (Exception e) {
            log.warn("Ошибка при отправке батча хитов (размер: {}). Ошибка: {}. " +
                    "Пытаемся отправить по одному", hits.size(), e.getMessage());
            saveHitsIndividually(hits); // Fallback
        }
    }

    /**
     * Получает статистику за указанный период
     * @param start начальная дата
     * @param end конечная дата
     * @param uris список URI для фильтрации
     * @param unique учитывать только уникальные IP
     * @return список статистических данных
     */
    public List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end,
                                       List<String> uris, Boolean unique) {
        validateDates(start, end);

        try {
            // Разбиваем на несколько запросов если слишком много URI
            if (uris != null && uris.size() > MAX_URIS_PER_REQUEST) {
                return getStatsInBatches(start, end, uris, unique);
            }

            URI uri = buildStatsUri(start, end, uris, unique);

            log.debug("Получение статистики: start={}, end={}, uris={}, unique={}",
                    start, end, uris, unique);

            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

            ResponseEntity<ViewStatsDTO[]> response = simpleRestTemplate.exchange(
                    uri, HttpMethod.GET, requestEntity, ViewStatsDTO[].class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<ViewStatsDTO> stats = Arrays.asList(response.getBody());
                log.debug("Получено {} записей статистики", stats.size());
                return stats;
            } else {
                log.warn("Статистика не возвращена. Статус: {}", response.getStatusCode());
                return Collections.emptyList();
            }

        } catch (Exception e) {
            log.error("Ошибка при получении статистики: start={}, end={}, uris={}. Ошибка: {}",
                    start, end, uris, e.getMessage(), e);
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
                    .queryParam("unique", unique != null ? unique : false);

            if (uris != null && !uris.isEmpty()) {
                // Для нескольких URI передаем как повторяющийся параметр
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
        // Кэшируем инстанс на короткое время
        long currentTime = System.currentTimeMillis();
        if (cachedInstance != null && (currentTime - cacheTimestamp) < CACHE_TTL_MS) {
            return cachedInstance;
        }

        List<ServiceInstance> instances = discoveryClient.getInstances(STATS_SERVICE_ID);

        if (instances == null || instances.isEmpty()) {
            String errorMsg = "Статистический сервис (" + STATS_SERVICE_ID + ") не найден в реестре Eureka";
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        // Простой load balancing: выбираем случайный инстанс
        ServiceInstance instance = instances.get(random.nextInt(instances.size()));

        cachedInstance = instance;
        cacheTimestamp = currentTime;

        log.debug("Найден статистический сервис: {}:{}. Всего инстансов: {}",
                instance.getHost(), instance.getPort(), instances.size());

        return instance;
    }

    private void validateDates(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) {
            throw new IllegalArgumentException("Даты не могут быть null");
        }
        if (start.isAfter(end)) {
            throw new IllegalArgumentException("Начальная дата должна быть раньше конечной даты");
        }
    }

    /**
     * Отправляет хиты по одному в случае проблем с батчем
     */
    private void saveHitsIndividually(List<EndpointHitDTO> hits) {
        int successCount = 0;
        int errorCount = 0;

        for (EndpointHitDTO hit : hits) {
            try {
                saveHit(hit);
                successCount++;
            } catch (Exception e) {
                errorCount++;
                log.error("Не удалось отправить хит в fallback режиме: {}", e.getMessage());
            }
        }

        log.info("Fallback отправка завершена: успешно={}, с ошибками={}, всего={}",
                successCount, errorCount, hits.size());
    }

    /**
     * Разбивает большой батч на части и отправляет
     */
    private void saveHitsInBatches(List<EndpointHitDTO> hits) {
        int totalBatches = (int) Math.ceil((double) hits.size() / MAX_BATCH_SIZE);

        for (int i = 0; i < totalBatches; i++) {
            int fromIndex = i * MAX_BATCH_SIZE;
            int toIndex = Math.min(fromIndex + MAX_BATCH_SIZE, hits.size());
            List<EndpointHitDTO> batch = hits.subList(fromIndex, toIndex);

            try {
                saveHits(batch);
                log.debug("Отправлен батч {}/{} (размер: {})", i + 1, totalBatches, batch.size());
            } catch (Exception e) {
                log.error("Ошибка при отправке батча {}/{}. Ошибка: {}",
                        i + 1, totalBatches, e.getMessage());
            }
        }
    }

    /**
     * Получает статистику по частям если слишком много URI
     */
    private List<ViewStatsDTO> getStatsInBatches(LocalDateTime start, LocalDateTime end,
                                                 List<String> uris, Boolean unique) {
        List<ViewStatsDTO> allStats = new ArrayList<>();
        int totalBatches = (int) Math.ceil((double) uris.size() / MAX_URIS_PER_REQUEST);

        for (int i = 0; i < totalBatches; i++) {
            int fromIndex = i * MAX_URIS_PER_REQUEST;
            int toIndex = Math.min(fromIndex + MAX_URIS_PER_REQUEST, uris.size());
            List<String> batchUris = uris.subList(fromIndex, toIndex);

            try {
                List<ViewStatsDTO> batchStats = getStats(start, end, batchUris, unique);
                allStats.addAll(batchStats);
                log.debug("Получена статистика для батча URI {}/{} (размер: {})",
                        i + 1, totalBatches, batchUris.size());
            } catch (Exception e) {
                log.error("Ошибка при получении статистики для батча URI {}/{}. Ошибка: {}",
                        i + 1, totalBatches, e.getMessage());
            }
        }

        return allStats;
    }

    /**
     * Создает EndpointHitDTO для стандартных запросов
     */
    public EndpointHitDTO createHitDto(String uri, String ip) {
        return EndpointHitDTO.builder()
                .app(APP_NAME)
                .uri(uri)
                .ip(ip)
                .timestamp(LocalDateTime.now().format(FORMATTER))
                .build();
    }
}