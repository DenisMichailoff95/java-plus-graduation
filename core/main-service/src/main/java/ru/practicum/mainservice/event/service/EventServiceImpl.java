package ru.practicum.mainservice.event.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.dto.ViewStatsDTO;
import ru.practicum.mainservice.category.model.Category;
import ru.practicum.mainservice.category.repository.CategoryRepository;
import ru.practicum.mainservice.event.dto.EventCreateDto;
import ru.practicum.mainservice.event.dto.EventDtoOut;
import ru.practicum.mainservice.event.dto.EventShortDtoOut;
import ru.practicum.mainservice.event.dto.EventUpdateAdminDto;
import ru.practicum.mainservice.event.dto.EventUpdateDto;
import ru.practicum.mainservice.event.mapper.EventMapper;
import ru.practicum.mainservice.event.model.Event;
import ru.practicum.mainservice.event.model.EventAdminFilter;
import ru.practicum.mainservice.event.model.EventFilter;
import ru.practicum.mainservice.event.model.EventState;
import ru.practicum.mainservice.event.repository.EventRepository;
import ru.practicum.mainservice.exception.ConditionNotMetException;
import ru.practicum.mainservice.exception.NoAccessException;
import ru.practicum.mainservice.exception.NotFoundException;
import ru.practicum.mainservice.participation.repository.ParticipationRequestRepository;
import ru.practicum.mainservice.statsclient.StatsClientImpl;
import ru.practicum.mainservice.user.model.User;
import ru.practicum.mainservice.user.repository.UserRepository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.practicum.mainservice.constants.Constants.STATS_EVENTS_URL;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class EventServiceImpl implements EventService {

    private static final int MIN_TIME_TO_UNPUBLISHED_EVENT = 2;
    private static final int MIN_TIME_TO_PUBLISHED_EVENT = 1;
    private static final int VIEWS_CACHE_DURATION_MINUTES = 5;

    private final EventRepository eventRepository;
    private final UserRepository userRepository;
    private final CategoryRepository categoryRepository;
    private final ParticipationRequestRepository requestRepository;
    private final StatsClientImpl statsClient;

    @Override
    @Transactional
    public EventDtoOut add(Long userId, EventCreateDto eventDto) {
        log.debug("Creating event for user {}: {}", userId, eventDto.getTitle());

        validateEventDate(eventDto.getEventDate(), EventState.PENDING);
        Category category = getCategory(eventDto.getCategoryId());
        User user = getUser(userId);

        Event event = EventMapper.fromDto(eventDto);
        event.setCategory(category);
        event.setInitiator(user);
        event.setState(EventState.PENDING);

        Event saved = eventRepository.save(event);
        log.info("Event created: id={}, title={}, user={}",
                saved.getId(), saved.getTitle(), userId);

        return EventMapper.toDto(saved);
    }

    @Override
    @Transactional
    public EventDtoOut update(Long userId, Long eventId, EventUpdateDto eventDto) {
        log.debug("Updating event {} by user {}", eventId, userId);

        Event event = getEvent(eventId);

        if (!event.getInitiator().getId().equals(userId)) {
            throw new NoAccessException("Only event initiator can update event");
        }

        if (event.getState() == EventState.PUBLISHED) {
            throw new ConditionNotMetException("Cannot update published event");
        }

        updateEventFields(event, eventDto);

        if (eventDto.getStateAction() != null) {
            updateEventState(event, eventDto.getStateAction());
        }

        Event updated = eventRepository.save(event);
        log.info("Event updated: id={}, newState={}", eventId, updated.getState());

        return EventMapper.toDto(updated);
    }

    @Override
    @Transactional
    public EventDtoOut update(Long eventId, EventUpdateAdminDto eventDto) {
        log.debug("Admin updating event {}", eventId);

        Event event = getEvent(eventId);
        updateEventFields(event, eventDto);

        if (eventDto.getStateAction() != null) {
            handleAdminStateAction(event, eventDto.getStateAction());
        }

        Event saved = eventRepository.save(event);
        log.info("Admin updated event: id={}, newState={}", eventId, saved.getState());

        return EventMapper.toDto(saved);
    }

    @Override
    @Cacheable(value = "publishedEvents", key = "#eventId", unless = "#result == null")
    public EventDtoOut findPublished(Long eventId) {
        log.debug("Finding published event: {}", eventId);

        Event event = eventRepository.findPublishedById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));

        enrichWithStats(Collections.singletonList(event));
        log.debug("Found published event: {}", eventId);

        return EventMapper.toDto(event);
    }

    @Override
    public EventDtoOut find(Long userId, Long eventId) {
        log.debug("Finding event {} for user {}", eventId, userId);

        validateUserExists(userId);
        Event event = getEvent(eventId);

        if (!event.getInitiator().getId().equals(userId)) {
            throw new NoAccessException("Only event initiator can view this event");
        }

        enrichWithStats(Collections.singletonList(event));

        return EventMapper.toDto(event);
    }

    @Override
    public Collection<EventShortDtoOut> findShortEventsBy(EventFilter filter) {
        log.debug("Finding short events with filter: {}", filter);

        Specification<Event> spec = buildSpecification(filter);
        Collection<Event> events = findBy(spec, filter.getPageable());

        return events.stream()
                .map(EventMapper::toShortDto)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<EventDtoOut> findFullEventsBy(EventAdminFilter filter) {
        log.debug("Admin finding full events with filter: {}", filter);

        Specification<Event> spec = buildSpecification(filter);
        Collection<Event> events = findBy(spec, filter.getPageable());

        return events.stream()
                .map(EventMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<EventShortDtoOut> findByInitiator(Long userId, Integer offset, Integer limit) {
        log.debug("Finding events for initiator: {}", userId);

        validateUserExists(userId);

        Pageable pageable = PageRequest.of(offset / limit, limit, Sort.by("eventDate").descending());
        Page<Event> eventPage = eventRepository.findByInitiatorId(userId, pageable);
        List<Event> events = eventPage.getContent();

        enrichWithStatsCollection(events);

        return events.stream()
                .map(EventMapper::toShortDto)
                .collect(Collectors.toList());
    }

    // Private helper methods

    private void updateEventFields(Event event, EventUpdateDto eventDto) {
        Optional.ofNullable(eventDto.getTitle()).ifPresent(event::setTitle);
        Optional.ofNullable(eventDto.getAnnotation()).ifPresent(event::setAnnotation);
        Optional.ofNullable(eventDto.getDescription()).ifPresent(event::setDescription);
        Optional.ofNullable(eventDto.getPaid()).ifPresent(event::setPaid);

        Optional.ofNullable(eventDto.getLocation()).ifPresent(loc -> {
            event.setLocationLat(loc.getLat());
            event.setLocationLon(loc.getLon());
        });

        Optional.ofNullable(eventDto.getParticipantLimit()).ifPresent(event::setParticipantLimit);
        Optional.ofNullable(eventDto.getRequestModeration()).ifPresent(event::setRequestModeration);

        if (eventDto.getCategoryId() != null
                && !eventDto.getCategoryId().equals(event.getCategory().getId())) {
            Category category = categoryRepository.findById(eventDto.getCategoryId())
                    .orElseThrow(() -> new NotFoundException("Category", eventDto.getCategoryId()));
            event.setCategory(category);
        }

        if (eventDto.getEventDate() != null) {
            validateEventDate(eventDto.getEventDate(), event.getState());
            event.setEventDate(eventDto.getEventDate());
        }
    }

    private void updateEventFields(Event event, EventUpdateAdminDto eventDto) {
        Optional.ofNullable(eventDto.getTitle()).ifPresent(event::setTitle);
        Optional.ofNullable(eventDto.getAnnotation()).ifPresent(event::setAnnotation);
        Optional.ofNullable(eventDto.getDescription()).ifPresent(event::setDescription);
        Optional.ofNullable(eventDto.getParticipantLimit()).ifPresent(event::setParticipantLimit);
        Optional.ofNullable(eventDto.getPaid()).ifPresent(event::setPaid);

        Optional.ofNullable(eventDto.getLocation()).ifPresent(loc -> {
            event.setLocationLat(loc.getLat());
            event.setLocationLon(loc.getLon());
        });

        Optional.ofNullable(eventDto.getRequestModeration()).ifPresent(event::setRequestModeration);

        if (eventDto.getEventDate() != null) {
            validateEventDate(eventDto.getEventDate(), event.getState());
            event.setEventDate(eventDto.getEventDate());
        }
    }

    private void updateEventState(Event event, EventUpdateDto.StateAction stateAction) {
        switch (stateAction) {
            case SEND_TO_REVIEW -> event.setState(EventState.PENDING);
            case CANCEL_REVIEW -> event.setState(EventState.CANCELED);
        }
    }

    private void handleAdminStateAction(Event event, EventUpdateAdminDto.StateAction stateAction) {
        switch (stateAction) {
            case PUBLISH_EVENT -> publishEvent(event);
            case REJECT_EVENT -> rejectEvent(event);
        }
    }

    private void validateUserExists(Long userId) {
        if (!userRepository.existsById(userId)) {
            throw new NotFoundException("User", userId);
        }
    }

    private void enrichWithStats(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        enrichEventsWithConfirmedRequests(events);
        enrichWithViewsCountCollection(events);
    }

    void enrichWithStatsCollection(Collection<Event> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        List<Event> eventList = new ArrayList<>(events);
        enrichEventsWithConfirmedRequests(eventList);
        enrichWithViewsCountCollection(eventList);
    }

    private void enrichWithViewsCountCollection(Collection<Event> events) {
        if (events.isEmpty()) {
            return;
        }

        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .collect(Collectors.toList());

        Map<Long, Long> eventViewsMap = getViewsCountForEvents(eventIds);

        events.forEach(event ->
                event.setViews(eventViewsMap.getOrDefault(event.getId(), 0L))
        );
    }

    private Map<Long, Long> getViewsCountForEvents(List<Long> eventIds) {
        if (eventIds.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> uris = eventIds.stream()
                .map(id -> STATS_EVENTS_URL + id)
                .collect(Collectors.toList());

        log.debug("Getting stats for {} events from stats server", eventIds.size());

        try {
            LocalDateTime start = LocalDateTime.now().minusDays(7);
            LocalDateTime end = LocalDateTime.now();

            List<ViewStatsDTO> stats = statsClient.getStats(start, end, uris, true);

            log.debug("Received stats for {} uris", stats.size());

            return stats.stream()
                    .collect(Collectors.toMap(
                            stat -> extractEventIdFromUri(stat.getUri()),
                            ViewStatsDTO::getHits
                    ));
        } catch (Exception e) {
            log.error("Failed to get stats from stats server: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    private Long extractEventIdFromUri(String uri) {
        try {
            return Long.parseLong(uri.substring(STATS_EVENTS_URL.length()));
        } catch (NumberFormatException e) {
            log.warn("Failed to extract eventId from uri: {}", uri);
            return -1L;
        }
    }

    @Transactional(readOnly = true)
    void enrichEventsWithConfirmedRequests(Collection<Event> events) {
        if (events == null || events.isEmpty()) {
            return;
        }

        Map<Long, Integer> confirmedRequestsCounts = getConfirmedRequestsCountsByEventIds(events);
        applyConfirmedRequestsCountsToEvents(events, confirmedRequestsCounts);
    }

    private Map<Long, Integer> getConfirmedRequestsCountsByEventIds(Collection<Event> events) {
        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .collect(Collectors.toList());

        if (eventIds.isEmpty()) {
            return Collections.emptyMap();
        }

        List<Object[]> requestsCountsList = requestRepository.findConfirmedRequestCountsByEventIds(eventIds);

        return requestsCountsList.stream()
                .collect(Collectors.toMap(
                        arr -> ((Number) arr[0]).longValue(),
                        arr -> ((Number) arr[1]).intValue()
                ));
    }

    private void applyConfirmedRequestsCountsToEvents(Collection<Event> events, Map<Long, Integer> countsMap) {
        events.forEach(event -> {
            Integer count = countsMap.getOrDefault(event.getId(), 0);
            event.setConfirmedRequests(count);
        });
    }

    private void validateEventDate(LocalDateTime eventDate, EventState state) {
        if (eventDate == null) {
            throw new IllegalArgumentException("Event date cannot be null");
        }

        int hours = state == EventState.PUBLISHED
                ? MIN_TIME_TO_PUBLISHED_EVENT
                : MIN_TIME_TO_UNPUBLISHED_EVENT;

        LocalDateTime minAllowedDate = LocalDateTime.now().plusHours(hours);

        if (eventDate.isBefore(minAllowedDate)) {
            String message = String.format(
                    "Event date must be at least %d hours from now for %s events",
                    hours, state == EventState.PUBLISHED ? "publishing" : "creating/updating"
            );
            throw new ConditionNotMetException(message);
        }
    }

    private Category getCategory(Long categoryId) {
        return categoryRepository.findById(categoryId)
                .orElseThrow(() -> new NotFoundException("Category", categoryId));
    }

    private User getUser(Long userId) {
        return userRepository.findById(userId)
                .orElseThrow(() -> new NotFoundException("User", userId));
    }

    private Event getEvent(Long eventId) {
        return eventRepository.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));
    }

    private void publishEvent(Event event) {
        if (event.getState() != EventState.PENDING) {
            throw new ConditionNotMetException("Only pending events can be published");
        }

        validateEventDate(event.getEventDate(), EventState.PUBLISHED);
        event.setState(EventState.PUBLISHED);
        event.setPublishedOn(LocalDateTime.now());
    }

    private void rejectEvent(Event event) {
        if (event.getState() == EventState.PUBLISHED) {
            throw new ConditionNotMetException("Published events cannot be rejected");
        }
        event.setState(EventState.CANCELED);
    }

    private Collection<Event> findBy(Specification<Event> spec, Pageable pageable) {
        Page<Event> page = eventRepository.findAll(spec, pageable);
        List<Event> events = page.getContent();
        enrichWithStatsCollection(events);
        return events;
    }

    private Specification<Event> buildSpecification(EventAdminFilter filter) {
        return Stream.of(
                        optionalSpec(EventSpecifications.withUsers(filter.getUsers())),
                        optionalSpec(EventSpecifications.withCategoriesIn(filter.getCategories())),
                        optionalSpec(EventSpecifications.withStatesIn(filter.getStates())),
                        optionalSpec(EventSpecifications.withRangeStart(filter.getRangeStart())),
                        optionalSpec(EventSpecifications.withRangeEnd(filter.getRangeEnd()))
                )
                .filter(Objects::nonNull)
                .reduce(Specification::and)
                .orElse((root, query, cb) -> cb.conjunction());
    }

    private Specification<Event> buildSpecification(EventFilter filter) {
        return Stream.of(
                        optionalSpec(EventSpecifications.withTextContains(filter.getText())),
                        optionalSpec(EventSpecifications.withCategoriesIn(filter.getCategories())),
                        optionalSpec(EventSpecifications.withPaid(filter.getPaid())),
                        optionalSpec(EventSpecifications.withState(filter.getState())),
                        optionalSpec(EventSpecifications.withOnlyAvailable(filter.getOnlyAvailable())),
                        optionalSpec(EventSpecifications.withRangeStart(filter.getRangeStart())),
                        optionalSpec(EventSpecifications.withRangeEnd(filter.getRangeEnd()))
                )
                .filter(Objects::nonNull)
                .reduce(Specification::and)
                .orElse((root, query, cb) -> cb.conjunction());
    }

    private static <T> Specification<T> optionalSpec(Specification<T> spec) {
        return spec;
    }
}