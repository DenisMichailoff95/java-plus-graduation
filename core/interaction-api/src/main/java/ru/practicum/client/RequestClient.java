package ru.practicum.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import ru.practicum.dto.participation.ParticipationRequestDto;

import java.util.List;
import java.util.Map;

@FeignClient(name = "request-service", contextId = "requestClient", path = "/internal/requests")
@CircuitBreaker(name = "request-service")
@Retry(name = "request-service")
public interface RequestClient {

    @GetMapping("/event/{eventId}/count")
    Integer getConfirmedRequestsCount(@PathVariable Long eventId);

    @GetMapping("/event/{eventId}")
    List<ParticipationRequestDto> getRequestsForEvent(@PathVariable Long eventId);

    @GetMapping("/user/{userId}/event/{eventId}/exists")
    Boolean userAlreadyParticipates(@PathVariable Long userId, @PathVariable Long eventId);

    @PostMapping("/event/{eventId}/confirmed")
    void incrementConfirmedRequests(@PathVariable Long eventId);

    @GetMapping("/batch/count")
    Map<Long, Integer> getConfirmedRequestsCounts(@RequestParam List<Long> eventIds);
}