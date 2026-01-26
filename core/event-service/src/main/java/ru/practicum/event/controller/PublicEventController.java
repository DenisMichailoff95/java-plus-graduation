package ru.practicum.event.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.enums.EventState;
import ru.practicum.event.model.EventFilter;
import ru.practicum.event.service.EventService;
import ru.practicum.exception.InvalidRequestException;
import ru.practicum.statsclient.EventStatsClient;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static ru.practicum.constants.Constants.DATE_TIME_FORMAT;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class PublicEventController {

    private final EventService eventService;
    private final EventStatsClient eventStatsClient;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @GetMapping
    public List<EventShortDtoOut> getEvents(
            @Size(min = 3, max = 1000, message = "Текст должен быть длиной от 3 до 1000 символов")
            @RequestParam(required = false) String text,
            @RequestParam(required = false) List<Long> categories,
            @RequestParam(required = false) Boolean paid,
            @RequestParam(required = false) @DateTimeFormat(pattern = DATE_TIME_FORMAT) LocalDateTime rangeStart,
            @RequestParam(required = false) @DateTimeFormat(pattern = DATE_TIME_FORMAT) LocalDateTime rangeEnd,
            @RequestParam(defaultValue = "false") Boolean onlyAvailable,
            @RequestParam(defaultValue = "EVENT_DATE") String sort,
            @RequestParam(defaultValue = "0") Integer from,
            @RequestParam(defaultValue = "10") Integer size,
            HttpServletRequest request) {

        log.info("Public request to get events: text={}, categories={}, from={}, size={}",
                text, categories, from, size);

        EventFilter filter = EventFilter.builder()
                .text(text)
                .categories(categories)
                .paid(paid)
                .rangeStart(rangeStart)
                .rangeEnd(rangeEnd)
                .onlyAvailable(onlyAvailable)
                .sort(sort)
                .from(from)
                .size(size)
                .state(EventState.PUBLISHED)
                .build();

        if (filter.getRangeStart() != null && filter.getRangeEnd() != null) {
            if (filter.getRangeStart().isAfter(filter.getRangeEnd())) {
                throw new InvalidRequestException("Дата начала должна быть раньше даты конца");
            }
        }

        List<EventShortDtoOut> events = eventService.findShortEventsBy(filter);

        // Отправляем статистику для списка событий
        if (!events.isEmpty()) {
            String clientIp = getClientIp(request);
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            // Отправляем хит для списка событий
            String listUri = request.getRequestURI() + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

            EndpointHitDTO listHitDto = EndpointHitDTO.builder()
                    .app("event-service")
                    .uri(listUri)
                    .ip(clientIp)
                    .timestamp(timestamp)
                    .build();
            eventStatsClient.saveHit(listHitDto);

            log.debug("Sent hit for events list: uri={}, ip={}", listUri, clientIp);
        }

        return events;
    }

    @GetMapping("/{eventId}")
    public EventDtoOut get(@PathVariable @Min(1) Long eventId,
                           HttpServletRequest request) {
        log.info("Public request to get event: id={}", eventId);

        // Получаем событие
        EventDtoOut dtoOut = eventService.findPublished(eventId);

        // Отправляем статистику
        String clientIp = getClientIp(request);
        String uri = "/events/" + eventId;

        log.info("Sending hit for event {} from IP: {}", eventId, clientIp);

        EndpointHitDTO eventHitDto = EndpointHitDTO.builder()
                .app("event-service")
                .uri(uri)
                .ip(clientIp)
                .timestamp(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .build();
        eventStatsClient.saveHit(eventHitDto);

        return dtoOut;
    }

    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }

        // Если несколько IP через запятую (цепочка прокси), берем первый
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }

        // Для тестирования, если IP localhost, заменяем на тестовый
        if (ip == null || ip.isEmpty() || "0:0:0:0:0:0:0:1".equals(ip) || "127.0.0.1".equals(ip)) {
            ip = "192.168.1." + (int) (Math.random() * 255); // Генерируем тестовый IP
        }

        return ip;
    }
}