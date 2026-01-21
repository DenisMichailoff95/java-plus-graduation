package ru.practicum.mainservice.participation.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.mainservice.participation.dto.EventRequestStatusUpdateRequest;
import ru.practicum.mainservice.participation.dto.EventRequestStatusUpdateResult;
import ru.practicum.mainservice.participation.dto.ParticipationRequestDto;
import ru.practicum.mainservice.participation.service.ParticipationRequestService;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/users/{userId}/events/{eventId}/requests")
@RequiredArgsConstructor
@Validated
public class PrivateEventRequestController {
    private final ParticipationRequestService requestService;

    @PatchMapping
    public EventRequestStatusUpdateResult updateRequestStatuses(
            @PathVariable @Min(1) Long userId,
            @PathVariable @Min(1) Long eventId,
            @RequestBody @Valid EventRequestStatusUpdateRequest request) {
        log.info("PATCH /users/{}/events/{}/requests with body {}", userId, eventId, request);
        return requestService.updateRequestStatuses(userId, eventId, request);
    }

    @GetMapping
    public List<ParticipationRequestDto> getRequests(@PathVariable @Min(1) Long userId,
                                                     @PathVariable @Min(1) Long eventId) {
        log.info("GET /users/{}/events/{}/requests", userId, eventId);
        return requestService.getRequestsForEvent(eventId, userId);
    }
}
