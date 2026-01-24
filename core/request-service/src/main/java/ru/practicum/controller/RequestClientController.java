package ru.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.client.RequestClient;
import ru.practicum.dto.participation.ParticipationRequestDto;
import ru.practicum.mapper.ParticipationRequestMapper;
import ru.practicum.repository.ParticipationRequestRepository;
import ru.practicum.enums.RequestStatus;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/requests")
public class RequestClientController implements RequestClient {

    private final ParticipationRequestRepository requestRepository;

    @Override
    @GetMapping("/event/{eventId}/count")
    public Integer getConfirmedRequestsCount(@PathVariable Long eventId) {
        return requestRepository.countByEventIdAndStatus(eventId, RequestStatus.CONFIRMED);
    }

    @Override
    @GetMapping("/event/{eventId}")
    public List<ParticipationRequestDto> getRequestsForEvent(@PathVariable Long eventId) {
        return requestRepository.findAllByEventId(eventId).stream()
                .map(ParticipationRequestMapper::toDto)
                .toList();
    }

    @Override
    @GetMapping("/user/{userId}/event/{eventId}/exists")
    public Boolean userAlreadyParticipates(@PathVariable Long userId, @PathVariable Long eventId) {
        return requestRepository.existsByRequesterIdAndEventId(userId, eventId);
    }

    @Override
    @PostMapping("/event/{eventId}/confirmed")
    public void incrementConfirmedRequests(@PathVariable Long eventId) {
        log.info("Increment confirmed requests for event {}", eventId);
    }

    @GetMapping("/batch/count")
    public Map<Long, Integer> getConfirmedRequestsCounts(@RequestParam List<Long> eventIds) {
        return requestRepository.findConfirmedRequestCountsByEventIds(eventIds).stream()
                .collect(Collectors.toMap(
                        arr -> ((Number) arr[0]).longValue(),
                        arr -> ((Number) arr[1]).intValue()
                ));
    }
}