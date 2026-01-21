package ru.practicum.mainservice.event.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.mainservice.event.dto.EventDtoOut;
import ru.practicum.mainservice.event.dto.EventShortDtoOut;
import ru.practicum.mainservice.event.model.EventFilter;
import ru.practicum.mainservice.event.model.EventState;
import ru.practicum.mainservice.event.service.EventService;
import ru.practicum.mainservice.exception.InvalidRequestException;
import ru.practicum.mainservice.statsclient.StatsClientImpl;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static ru.practicum.mainservice.constants.Constants.DATE_TIME_FORMAT;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class PublicEventController {

    private final EventService eventService;
    private final StatsClientImpl statsClient;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @GetMapping
    public Collection<EventShortDtoOut> getEvents(
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

        Collection<EventShortDtoOut> events = eventService.findShortEventsBy(filter);

        if (!events.isEmpty()) {
            sendStatsForEvents(events, request);
        }

        sendStatsForEventsList(request);

        return events;
    }

    @GetMapping("/{eventId}")
    public EventDtoOut get(@PathVariable @Min(1) Long eventId,
                           HttpServletRequest request) {
        log.info("=== PUBLIC EVENT CONTROLLER: Processing event {} ===", eventId);

        EventDtoOut dtoOut = eventService.findPublished(eventId);

        String clientIp = getClientIp(request);
        String timestamp = LocalDateTime.now().format(FORMATTER);

        EndpointHitDTO endpointHitDto = EndpointHitDTO.builder()
                .app("ewm-main-service")
                .uri("/events/" + eventId)
                .ip(clientIp)
                .timestamp(timestamp)
                .build();

        log.info("=== SENDING HIT to stats-server: {}", endpointHitDto);

        statsClient.saveHit(endpointHitDto);

        log.info("=== HIT SENT ===");

        return dtoOut;
    }

    private void sendStatsForEvents(Collection<EventShortDtoOut> events, HttpServletRequest request) {
        String clientIp = getClientIp(request);
        String timestamp = LocalDateTime.now().format(FORMATTER);

        List<EndpointHitDTO> hits = events.stream()
                .map(event -> EndpointHitDTO.builder()
                        .app("ewm-main-service")
                        .uri("/events/" + event.getId())
                        .ip(clientIp)
                        .timestamp(timestamp)
                        .build())
                .collect(Collectors.toList());

        log.debug("Sending batch hits for {} events", hits.size());
        statsClient.saveHits(hits);
    }

    private void sendStatsForEventsList(HttpServletRequest request) {
        String clientIp = getClientIp(request);
        String timestamp = LocalDateTime.now().format(FORMATTER);

        EndpointHitDTO listHit = EndpointHitDTO.builder()
                .app("ewm-main-service")
                .uri("/events")
                .ip(clientIp)
                .timestamp(timestamp)
                .build();

        log.debug("Sending hit for events list request");
        statsClient.saveHit(listHit);
    }

    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }
}

