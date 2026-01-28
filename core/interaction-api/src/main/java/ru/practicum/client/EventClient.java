package ru.practicum.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;

import java.util.List;

@FeignClient(name = "event-service", contextId = "eventClient", path = "/internal/events")
@CircuitBreaker(name = "event-service")
@Retry(name = "event-service")
public interface EventClient {

    @GetMapping("/{eventId}")
    EventDtoOut getEventById(@PathVariable Long eventId);

    @GetMapping("/short/{eventId}")
    EventShortDtoOut getShortEventById(@PathVariable Long eventId);

    @GetMapping("/exists/{eventId}")
    Boolean eventExists(@PathVariable Long eventId);

    @GetMapping("/batch")
    List<EventShortDtoOut> getEventsByIds(@RequestParam List<Long> eventIds);

    @GetMapping("/batch/full")
    List<EventDtoOut> getFullEventsByIds(@RequestParam List<Long> eventIds);

    @PutMapping("/{eventId}/increment")
    void incrementConfirmedRequests(@PathVariable Long eventId,
                                    @RequestParam(required = false, defaultValue = "1") int count);
}