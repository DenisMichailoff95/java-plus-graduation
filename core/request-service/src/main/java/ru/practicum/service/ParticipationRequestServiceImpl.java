package ru.practicum.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.client.EventClient;
import ru.practicum.client.UserClient;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.participation.EventRequestStatusUpdateRequest;
import ru.practicum.dto.participation.EventRequestStatusUpdateResult;
import ru.practicum.dto.participation.ParticipationRequestDto;
import ru.practicum.enums.EventState;
import ru.practicum.enums.RequestStatus;
import ru.practicum.exception.ConditionNotMetException;
import ru.practicum.exception.ForbiddenException;
import ru.practicum.exception.NoAccessException;
import ru.practicum.exception.NotFoundException;
import ru.practicum.mapper.ParticipationRequestMapper;
import ru.practicum.model.ParticipationRequest;
import ru.practicum.repository.ParticipationRequestRepository;
import ru.practicum.statsclient.client.CollectorClient;

import java.util.ArrayList;
import java.util.List;

import static ru.practicum.enums.RequestStatus.CANCELED;
import static ru.practicum.enums.RequestStatus.CONFIRMED;


@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ParticipationRequestServiceImpl implements ParticipationRequestService {

    private final UserClient userClient;
    private final EventClient eventClient;
    private final ParticipationRequestRepository requestRepository;
    private final ParticipationRequestMapper requestMapper;
    private final CollectorClient collectorClient;

    @Override
    @Transactional
    public ParticipationRequestDto createRequest(Long userId, Long eventId) {
        log.info("=== START createRequest: userId={}, eventId={} ===", userId, eventId);

        if (!userExists(userId)) {
            throw new NotFoundException("User", userId);
        }

        EventDtoOut event = getEventById(eventId);
        if (event == null) {
            throw new NotFoundException("Event", eventId);
        }

        if (requestRepository.existsByRequesterIdAndEventId(userId, eventId)) {
            throw new ConditionNotMetException("Заявка на участие уже отправлена.");
        }

        if (event.getInitiator().getId().equals(userId)) {
            throw new ConditionNotMetException("Заявка на участие уже отправлена.");
        }

        if (!"PUBLISHED".equals(event.getState())) {
            throw new ConditionNotMetException("Невозможно принять участие в неопубликованном мероприятии.");
        }

        long confirmed = requestRepository.countByEventIdAndStatus(eventId, CONFIRMED);
        log.debug("Confirmed requests: {}, limit: {}", confirmed, event.getParticipantLimit());
        if (event.getParticipantLimit() > 0 && confirmed >= event.getParticipantLimit()) {
            throw new ConditionNotMetException("Лимит участников мероприятия достигнут.");
        }

        RequestStatus status = (!Boolean.TRUE.equals(event.getRequestModeration()) || event.getParticipantLimit() == 0)
                ? CONFIRMED
                : RequestStatus.PENDING;

        log.debug("Determined request status: {}", status);

        ParticipationRequest request = new ParticipationRequest();
        request.setRequesterId(userId);
        request.setEventId(eventId);
        request.setStatus(status);

        log.debug("Saving request: requesterId={}, eventId={}, status={}",
                request.getRequesterId(), request.getEventId(), request.getStatus());

        ParticipationRequest saved = requestRepository.save(request);
        log.info("Request saved: id={}, created={}", saved.getId(), saved.getCreated());

        if (status == CONFIRMED) {
            sendRegisterActionAsync(userId, eventId);
        }

        ParticipationRequestDto dto = requestMapper.toDto(saved);

        log.info("=== END createRequest: created requestId={} with status={} ===",
                saved.getId(), saved.getStatus());

        return dto;
    }

    @Override
    public List<ParticipationRequestDto> getUserRequests(Long userId) {
        if (!userExists(userId)) {
            throw new NotFoundException("User", userId);
        }
        return requestRepository.findAllByRequesterId(userId).stream()
                .map(requestMapper::toDto)
                .toList();
    }

    @Override
    @Transactional
    public EventRequestStatusUpdateResult updateRequestStatuses(Long userId, Long eventId,
                                                                EventRequestStatusUpdateRequest request) {
        if (!userExists(userId)) {
            throw new NotFoundException("User", userId);
        }

        EventDtoOut event = getEventWithCheck(userId, eventId);
        List<ParticipationRequest> requests = getPendingRequestsOrThrow(request.getRequestIds());

        return switch (request.getStatus()) {
            case "CONFIRMED" -> confirmRequests(event, requests);
            case "REJECTED" -> rejectRequests(requests);
            default -> throw new IllegalArgumentException("Неправильный статус: " + request.getStatus());
        };
    }

    @Override
    public List<ParticipationRequestDto> getRequestsForEvent(Long eventId, Long initiatorId) {
        log.debug("getRequestsForEvent: {} of user: {}", eventId, initiatorId);

        if (!userExists(initiatorId)) {
            throw new NotFoundException("User", initiatorId);
        }

        EventDtoOut event;
        try {
            event = eventClient.getEventById(eventId);
            if (event == null) {
                throw new NotFoundException("Event", eventId);
            }
        } catch (FeignException.NotFound e) {
            throw new NotFoundException("Event", eventId);
        } catch (FeignException e) {
            log.error("Feign error getting event: eventId={}, status={}", eventId, e.status());
            throw new RuntimeException("Event service unavailable: " + e.getMessage());
        }

        if (!event.getInitiator().getId().equals(initiatorId)) {
            throw new NoAccessException("Только инициатор может просматривать запросы на проведение мероприятия");
        }

        List<ParticipationRequest> allByEventId = requestRepository.findAllByEventId(eventId);
        return allByEventId.stream()
                .map(requestMapper::toDto)
                .toList();
    }

    @Override
    @Transactional
    public ParticipationRequestDto cancelRequest(Long userId, Long requestId) {
        log.info("Пользователь {} отменяет заявку с ID {}", userId, requestId);

        ParticipationRequest request = requestRepository.findById(requestId)
                .orElseThrow(() -> new NotFoundException("ParticipationRequest", requestId));

        if (!request.getRequesterId().equals(userId)) {
            throw new ForbiddenException("Отменить его может только автор заявки.");
        }

        request.setStatus(CANCELED);
        ParticipationRequest saved = requestRepository.save(request);
        return requestMapper.toDto(saved);
    }

    private boolean userExists(Long userId) {
        try {
            Boolean exists = userClient.userExists(userId);
            return exists != null && exists;
        } catch (FeignException.NotFound e) {
            log.warn("User not found via Feign: userId={}", userId);
            return false;
        } catch (FeignException e) {
            log.error("Error checking user existence via Feign: userId={}, status={}",
                    userId, e.status());
            return true;
        }
    }

    private EventDtoOut getEventWithCheck(Long userId, Long eventId) {
        EventDtoOut event;
        try {
            event = eventClient.getEventById(eventId);
            if (event == null) {
                throw new NotFoundException("Event", eventId);
            }
        } catch (FeignException.NotFound e) {
            throw new NotFoundException("Event", eventId);
        } catch (FeignException e) {
            log.error("Feign error getting event: eventId={}, status={}", eventId, e.status());
            throw new RuntimeException("Event service unavailable: " + e.getMessage());
        }

        if (!event.getInitiator().getId().equals(userId)) {
            throw new ForbiddenException("Пользователь не является инициатором события");
        }

        if (!EventState.PUBLISHED.name().equals(event.getState())) {
            throw new ConditionNotMetException("Мероприятие должно быть опубликовано");
        }

        return event;
    }

    private List<ParticipationRequest> getPendingRequestsOrThrow(List<Long> requestIds) {
        List<ParticipationRequest> requests = requestRepository.findAllById(requestIds);
        boolean hasNonPending = requests.stream()
                .anyMatch(r -> r.getStatus() != RequestStatus.PENDING);

        if (hasNonPending) {
            throw new ConditionNotMetException("Запрос должен иметь статус ОЖИДАЮЩИЙ");
        }

        return requests;
    }

    private EventRequestStatusUpdateResult confirmRequests(EventDtoOut event, List<ParticipationRequest> requests) {
        checkIfLimitAvailableOrThrow(event);
        int limit = event.getParticipantLimit();
        long confirmedCount = requestRepository.countByEventIdAndStatus(event.getId(), CONFIRMED);
        int available = limit - (int) confirmedCount;

        List<ParticipationRequest> confirmed = new ArrayList<>();
        List<ParticipationRequest> rejected = new ArrayList<>();

        for (ParticipationRequest request : requests) {
            if (shouldAutoConfirm(event)) {
                confirmRequest(request, confirmed);
                sendRegisterActionAsync(request.getRequesterId(), request.getEventId());
            } else if (available > 0) {
                confirmRequest(request, confirmed);
                available--;
                sendRegisterActionAsync(request.getRequesterId(), request.getEventId());
            } else {
                rejectRequest(request, rejected);
            }
        }

        requestRepository.saveAll(requests);
        List<ParticipationRequestDto> confirmedDtos = confirmed.stream()
                .map(requestMapper::toDto)
                .toList();

        List<ParticipationRequestDto> rejectedDtos = rejected.stream()
                .map(requestMapper::toDto)
                .toList();

        return new EventRequestStatusUpdateResult(confirmedDtos, rejectedDtos);
    }

    private void checkIfLimitAvailableOrThrow(EventDtoOut event) {
        int limit = event.getParticipantLimit();
        long confirmedCount = requestRepository.countByEventIdAndStatus(event.getId(), CONFIRMED);
        if (limit != 0 && Boolean.TRUE.equals(event.getRequestModeration()) && confirmedCount >= limit) {
            throw new ConditionNotMetException("Лимит участников мероприятия достигнет");
        }
    }

    private boolean shouldAutoConfirm(EventDtoOut event) {
        return event.getParticipantLimit() == 0 || Boolean.FALSE.equals(event.getRequestModeration());
    }

    private void confirmRequest(ParticipationRequest request, List<ParticipationRequest> confirmed) {
        request.setStatus(CONFIRMED);
        confirmed.add(request);
    }

    private void rejectRequest(ParticipationRequest request, List<ParticipationRequest> rejected) {
        request.setStatus(RequestStatus.REJECTED);
        rejected.add(request);
    }

    private EventRequestStatusUpdateResult rejectRequests(List<ParticipationRequest> requests) {
        for (ParticipationRequest r : requests) {
            r.setStatus(RequestStatus.REJECTED);
        }

        requestRepository.saveAll(requests);

        List<ParticipationRequestDto> rejectedDtos = requests.stream()
                .map(requestMapper::toDto)
                .toList();

        return new EventRequestStatusUpdateResult(List.of(), rejectedDtos);
    }

    private EventDtoOut getEventById(Long eventId) {
        try {
            return eventClient.getEventById(eventId);
        } catch (FeignException.NotFound e) {
            log.warn("Event not found via Feign: eventId={}", eventId);
            return null;
        } catch (FeignException e) {
            log.error("Error getting event via Feign: eventId={}, status={}", eventId, e.status());
            return createStubEvent(eventId);
        }
    }

    private EventDtoOut createStubEvent(Long eventId) {
        EventDtoOut event = new EventDtoOut();
        event.setId(eventId);
        event.setInitiator(ru.practicum.dto.user.UserDtoOut.builder().id(1L).build());
        event.setState("PUBLISHED");
        event.setParticipantLimit(10);
        event.setRequestModeration(true);
        return event;
    }

    private void sendRegisterActionAsync(Long userId, Long eventId) {
        new Thread(() -> {
            try {
                collectorClient.sendRegisterAction(userId, eventId);
                log.info("Sent REGISTER action for user {} event {}", userId, eventId);
            } catch (Exception e) {
                log.warn("Failed to send REGISTER action (async): user={}, event={}, error={}",
                        userId, eventId, e.getMessage());
            }
        }).start();
    }
}