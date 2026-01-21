package ru.practicum.mainservice.participation.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.mainservice.event.model.Event;
import ru.practicum.mainservice.event.model.EventState;
import ru.practicum.mainservice.event.repository.EventRepository;
import ru.practicum.mainservice.exception.ConditionNotMetException;
import ru.practicum.mainservice.exception.ForbiddenException;
import ru.practicum.mainservice.exception.NoAccessException;
import ru.practicum.mainservice.exception.NotFoundException;
import ru.practicum.mainservice.participation.dto.EventRequestStatusUpdateRequest;
import ru.practicum.mainservice.participation.dto.EventRequestStatusUpdateResult;
import ru.practicum.mainservice.participation.dto.ParticipationRequestDto;
import ru.practicum.mainservice.participation.mapper.ParticipationRequestMapper;
import ru.practicum.mainservice.participation.model.ParticipationRequest;
import ru.practicum.mainservice.participation.model.RequestStatus;
import ru.practicum.mainservice.participation.repository.ParticipationRequestRepository;
import ru.practicum.mainservice.user.model.User;
import ru.practicum.mainservice.user.repository.UserRepository;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static ru.practicum.mainservice.participation.model.RequestStatus.CANCELED;
import static ru.practicum.mainservice.participation.model.RequestStatus.CONFIRMED;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ParticipationRequestServiceImpl implements ParticipationRequestService {

    private final UserRepository userRepo;
    private final EventRepository eventRepo;
    private final ParticipationRequestRepository requestRepo;

    @Transactional
    public ParticipationRequestDto createRequest(Long userId, Long eventId) {
        log.info("Пользователь {} пытается создать запрос участия для события {}", userId, eventId);

        User user = getUserById(userId);
        Event event = getEventById(eventId);

        checkRequestNotExists(userId, eventId);
        checkNotEventInitiator(userId, event);
        checkEventIsPublished(event);
        checkParticipantLimit(event, eventId);

        RequestStatus status = determineRequestStatus(event);

        ParticipationRequest request = new ParticipationRequest();
        request.setRequester(user);
        request.setEvent(event);
        request.setCreated(LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
        request.setStatus(status);
        log.info("Создана заявка от пользователя {} на событие {} со статусом {}", userId, eventId, status);
        return ParticipationRequestMapper.toDto(requestRepo.save(request));
    }

    @Override
    public List<ParticipationRequestDto> getUserRequests(Long userId) {
        if (!userRepo.existsById(userId)) {
            throw new NotFoundException("User", userId);
        }
        return requestRepo.findAllByRequesterId(userId).stream()
                .map(ParticipationRequestMapper::toDto)
                .toList();
    }

    @Transactional
    public EventRequestStatusUpdateResult updateRequestStatuses(Long userId, Long eventId,
                                                                EventRequestStatusUpdateRequest request) {
        Event event = getEventWithCheck(userId, eventId);
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
        getUserById(initiatorId);
        Event event = getEventById(eventId);
        if (!event.getInitiator().getId().equals(initiatorId)) {
            throw new NoAccessException("Только инициатор может просматривать запросы на проведение мероприятия");
        }
        List<ParticipationRequest> allByEventId = requestRepo.findAllByEventId(eventId);
        return allByEventId.stream()
                .map(ParticipationRequestMapper::toDto)
                .toList();
    }

    @Override
    @Transactional
    public ParticipationRequestDto cancelRequest(Long userId, Long requestId) {
        log.info("Пользователь {} отменяет заявку с ID {}", userId, requestId);
        ParticipationRequest request = requestRepo.findById(requestId)
                .orElseThrow(() -> new NotFoundException("ParticipationRequest", requestId));
        if (!request.getRequester().getId().equals(userId)) {
            throw new ForbiddenException("Отменить его может только автор заявки.");
        }
        request.setStatus(CANCELED);
        return ParticipationRequestMapper.toDto(requestRepo.save(request));
    }

    private User getUserById(Long userId) {
        return userRepo.findById(userId)
                .orElseThrow(() -> new NotFoundException("User", userId));
    }

    private Event getEventById(Long eventId) {
        return eventRepo.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));
    }

    private void checkRequestNotExists(Long userId, Long eventId) {
        if (requestRepo.existsByRequesterIdAndEventId(userId, eventId)) {
            throw new ConditionNotMetException("Заявка на участие уже отправлена.");
        }
    }

    private void checkNotEventInitiator(Long userId, Event event) {
        if (event.getInitiator().getId().equals(userId)) {
            throw new ConditionNotMetException("Заявка на участие уже отправлена.");
        }
    }

    private void checkEventIsPublished(Event event) {
        if (!event.getState().equals(EventState.PUBLISHED)) {
            throw new ConditionNotMetException("Невозможно принять участие в неопубликованном мероприятии.");
        }
    }

    private void checkParticipantLimit(Event event, Long eventId) {
        long confirmed = requestRepo.countByEventIdAndStatus(eventId, CONFIRMED);
        if (event.getParticipantLimit() > 0 && confirmed >= event.getParticipantLimit()) {
            throw new ConditionNotMetException("Лимит участников мероприятия достигнут.");
        }
    }

    private RequestStatus determineRequestStatus(Event event) {
        return (!Boolean.TRUE.equals(event.getRequestModeration()) || event.getParticipantLimit() == 0)
                ? RequestStatus.CONFIRMED
                : RequestStatus.PENDING;
    }

    private Event getEventWithCheck(Long userId, Long eventId) {
        Event event = eventRepo.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));
        if (!event.getInitiator().getId().equals(userId)) {
            throw new ForbiddenException("Пользователь не является инициатором события");
        }
        if (!EventState.PUBLISHED.equals(event.getState())) {
            throw new ConditionNotMetException("Мероприятие должно быть опубликовано");
        }
        return event;
    }

    private List<ParticipationRequest> getPendingRequestsOrThrow(List<Long> requestIds) {
        List<ParticipationRequest> requests = requestRepo.findAllById(requestIds);
        boolean hasNonPending = requests.stream()
                .anyMatch(r -> r.getStatus() != RequestStatus.PENDING);
        if (hasNonPending) {
            throw new ConditionNotMetException("Запрос должен иметь статус ОЖИДАЮЩИЙ");
        }
        return requests;
    }

    private EventRequestStatusUpdateResult confirmRequests(Event event, List<ParticipationRequest> requests) {
        checkIfLimitAvailableOrThrow(event);
        int limit = event.getParticipantLimit();
        long confirmedCount = requestRepo.countByEventIdAndStatus(event.getId(), RequestStatus.CONFIRMED);
        int available = limit - (int) confirmedCount;
        List<ParticipationRequest> confirmed = new ArrayList<>();
        List<ParticipationRequest> rejected = new ArrayList<>();
        for (ParticipationRequest request : requests) {
            if (shouldAutoConfirm(event)) {
                confirmRequest(request, confirmed);
            } else if (available > 0) {
                confirmRequest(request, confirmed);
                available--;
            } else {
                rejectRequest(request, rejected);
            }
        }
        requestRepo.saveAll(requests);
        return new EventRequestStatusUpdateResult(
                confirmed.stream().map(ParticipationRequestMapper::toDto).toList(),
                rejected.stream().map(ParticipationRequestMapper::toDto).toList()
        );
    }

    private void checkIfLimitAvailableOrThrow(Event event) {
        int limit = event.getParticipantLimit();
        long confirmedCount = requestRepo.countByEventIdAndStatus(event.getId(), RequestStatus.CONFIRMED);
        if (limit != 0 && Boolean.TRUE.equals(event.getRequestModeration()) && confirmedCount >= limit) {
            throw new ConditionNotMetException("Лимит участников мероприятия достигнет");
        }
    }

    private boolean shouldAutoConfirm(Event event) {
        return event.getParticipantLimit() == 0 || Boolean.FALSE.equals(event.getRequestModeration());
    }

    private void confirmRequest(ParticipationRequest request, List<ParticipationRequest> confirmed) {
        request.setStatus(RequestStatus.CONFIRMED);
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
        requestRepo.saveAll(requests);
        return new EventRequestStatusUpdateResult(
                List.of(),
                requests.stream().map(ParticipationRequestMapper::toDto).toList()
        );
    }
}