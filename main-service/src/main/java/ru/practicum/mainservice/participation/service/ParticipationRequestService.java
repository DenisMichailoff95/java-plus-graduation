package ru.practicum.mainservice.participation.service;

import ru.practicum.mainservice.participation.dto.EventRequestStatusUpdateRequest;
import ru.practicum.mainservice.participation.dto.EventRequestStatusUpdateResult;
import ru.practicum.mainservice.participation.dto.ParticipationRequestDto;

import java.util.List;

public interface ParticipationRequestService {

    ParticipationRequestDto createRequest(Long userId, Long eventId);

    List<ParticipationRequestDto> getUserRequests(Long userId);

    List<ParticipationRequestDto> getRequestsForEvent(Long eventId, Long initiatorId);

    ParticipationRequestDto cancelRequest(Long userId, Long requestId);

    EventRequestStatusUpdateResult updateRequestStatuses(Long userId, Long eventId, EventRequestStatusUpdateRequest request);
}