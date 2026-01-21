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

import static ru.practicum.mainservice.participation.model.RequestStatus.*;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ParticipationRequestServiceImpl implements ParticipationRequestService {

    private final UserRepository userRepo;
    private final EventRepository eventRepo;
    private final ParticipationRequestRepository requestRepo;

    @Override
    @Transactional
    public ParticipationRequestDto createRequest(Long userId, Long eventId) {
        log.info("Creating participation request: userId={}, eventId={}", userId, eventId);

        User user = getUserById(userId);
        Event event = getEventById(eventId);

        log.debug("Event found: id={}, title={}, participantLimit={}, requestModeration={}",
                event.getId(), event.getTitle(), event.getParticipantLimit(), event.getRequestModeration());

        checkRequestNotExists(userId, eventId);
        checkNotEventInitiator(userId, event);
        checkEventIsPublished(event);
        checkParticipantLimit(event, eventId);

        RequestStatus status = determineRequestStatus(event);
        log.debug("Determined request status: {}", status);

        ParticipationRequest request = new ParticipationRequest();
        request.setRequester(user);
        request.setEvent(event);
        request.setCreated(LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
        request.setStatus(status);

        ParticipationRequest saved = requestRepo.save(request);
        log.info("Participation request created: id={}, userId={}, eventId={}, status={}",
                saved.getId(), userId, eventId, status);

        return ParticipationRequestMapper.toDto(saved);
    }

    @Override
    public List<ParticipationRequestDto> getUserRequests(Long userId) {
        log.debug("Getting requests for user: {}", userId);

        if (!userRepo.existsById(userId)) {
            throw new NotFoundException("User", userId);
        }

        List<ParticipationRequest> requests = requestRepo.findAllByRequesterId(userId);
        log.debug("Found {} requests for user: {}", requests.size(), userId);

        return requests.stream()
                .map(ParticipationRequestMapper::toDto)
                .toList();
    }

    @Override
    @Transactional
    public EventRequestStatusUpdateResult updateRequestStatuses(Long userId, Long eventId,
                                                                EventRequestStatusUpdateRequest request) {
        log.info("Updating request statuses: userId={}, eventId={}, status={}, requestIds={}",
                userId, eventId, request.getStatus(), request.getRequestIds());

        Event event = getEventWithCheck(userId, eventId);
        List<ParticipationRequest> requests = getPendingRequestsOrThrow(request.getRequestIds());

        EventRequestStatusUpdateResult result;
        if ("CONFIRMED".equals(request.getStatus())) {
            result = confirmRequests(event, requests);
        } else if ("REJECTED".equals(request.getStatus())) {
            result = rejectRequests(requests);
        } else {
            throw new IllegalArgumentException("Invalid status: " + request.getStatus());
        }

        log.info("Request statuses updated: confirmed={}, rejected={}",
                result.getConfirmedRequests().size(), result.getRejectedRequests().size());

        return result;
    }

    @Override
    public List<ParticipationRequestDto> getRequestsForEvent(Long eventId, Long initiatorId) {
        log.debug("Getting requests for event: {} by initiator: {}", eventId, initiatorId);

        getUserById(initiatorId);
        Event event = getEventById(eventId);

        if (!event.getInitiator().getId().equals(initiatorId)) {
            throw new NoAccessException("Only event initiator can view participation requests");
        }

        List<ParticipationRequest> requests = requestRepo.findAllByEventId(eventId);
        log.debug("Found {} requests for event: {}", requests.size(), eventId);

        return requests.stream()
                .map(ParticipationRequestMapper::toDto)
                .toList();
    }

    @Override
    @Transactional
    public ParticipationRequestDto cancelRequest(Long userId, Long requestId) {
        log.info("Canceling request: userId={}, requestId={}", userId, requestId);

        ParticipationRequest request = requestRepo.findById(requestId)
                .orElseThrow(() -> new NotFoundException("ParticipationRequest", requestId));

        if (!request.getRequester().getId().equals(userId)) {
            throw new ForbiddenException("Only request creator can cancel the request");
        }

        if (request.getStatus() == CANCELED) {
            throw new ConditionNotMetException("Request is already canceled");
        }

        request.setStatus(CANCELED);
        ParticipationRequest saved = requestRepo.save(request);

        log.info("Request canceled: id={}, userId={}", saved.getId(), userId);
        return ParticipationRequestMapper.toDto(saved);
    }

    // Private helper methods

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
            throw new ConditionNotMetException("Participation request already exists");
        }
    }

    private void checkNotEventInitiator(Long userId, Event event) {
        if (event.getInitiator().getId().equals(userId)) {
            throw new ConditionNotMetException("Event initiator cannot create participation request");
        }
    }

    private void checkEventIsPublished(Event event) {
        if (!EventState.PUBLISHED.equals(event.getState())) {
            throw new ConditionNotMetException("Cannot participate in unpublished event");
        }
    }

    private void checkParticipantLimit(Event event, Long eventId) {
        if (event.getParticipantLimit() != null && event.getParticipantLimit() > 0) {
            int confirmed = requestRepo.countByEventIdAndStatus(eventId, CONFIRMED);

            if (confirmed >= event.getParticipantLimit()) {
                throw new ConditionNotMetException("Participant limit reached");
            }
        }
    }

    private RequestStatus determineRequestStatus(Event event) {
        // Если лимит 0 или модерация отключена - сразу подтверждаем
        if (event.getParticipantLimit() != null && event.getParticipantLimit() == 0) {
            return CONFIRMED;
        }

        if (Boolean.FALSE.equals(event.getRequestModeration())) {
            return CONFIRMED;
        }

        return PENDING;
    }

    private Event getEventWithCheck(Long userId, Long eventId) {
        Event event = eventRepo.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));

        if (!event.getInitiator().getId().equals(userId)) {
            throw new ForbiddenException("User is not the event initiator");
        }

        if (!EventState.PUBLISHED.equals(event.getState())) {
            throw new ConditionNotMetException("Event must be published");
        }

        return event;
    }

    private List<ParticipationRequest> getPendingRequestsOrThrow(List<Long> requestIds) {
        List<ParticipationRequest> requests = requestRepo.findAllById(requestIds);

        if (requests.size() != requestIds.size()) {
            // Создаем исключение с сообщением, а не с id
            throw new NotFoundException("Some participation requests not found", -1L);
        }

        boolean hasNonPending = requests.stream()
                .anyMatch(r -> r.getStatus() != PENDING);

        if (hasNonPending) {
            throw new ConditionNotMetException("All requests must be PENDING");
        }

        return requests;
    }

    private EventRequestStatusUpdateResult confirmRequests(Event event, List<ParticipationRequest> requests) {
        int limit = event.getParticipantLimit() != null ? event.getParticipantLimit() : 0;
        int confirmedCount = requestRepo.countByEventIdAndStatus(event.getId(), CONFIRMED);
        int available = limit == 0 ? Integer.MAX_VALUE : limit - confirmedCount;

        List<ParticipationRequest> confirmed = new ArrayList<>();
        List<ParticipationRequest> rejected = new ArrayList<>();

        for (ParticipationRequest request : requests) {
            if (available > 0) {
                request.setStatus(CONFIRMED);
                confirmed.add(request);
                if (limit > 0) {
                    available--;
                }
            } else {
                request.setStatus(REJECTED);
                rejected.add(request);
            }
        }

        requestRepo.saveAll(requests);

        return new EventRequestStatusUpdateResult(
                confirmed.stream().map(ParticipationRequestMapper::toDto).toList(),
                rejected.stream().map(ParticipationRequestMapper::toDto).toList()
        );
    }

    private EventRequestStatusUpdateResult rejectRequests(List<ParticipationRequest> requests) {
        for (ParticipationRequest r : requests) {
            r.setStatus(REJECTED);
        }

        requestRepo.saveAll(requests);

        return new EventRequestStatusUpdateResult(
                List.of(),
                requests.stream().map(ParticipationRequestMapper::toDto).toList()
        );
    }
}