package ru.practicum.event.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.client.UserClient;
import ru.practicum.client.RequestClient;
import ru.practicum.client.CommentClient;
import ru.practicum.category.service.CategoryService;
import ru.practicum.dto.category.CategoryDtoOut;
import ru.practicum.dto.event.EventCreateDto;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.event.EventUpdateAdminDto;
import ru.practicum.dto.event.EventUpdateDto;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.statsclient.EventStatsClient;

import ru.practicum.enums.EventState;
import ru.practicum.event.mapper.EventMapper;
import ru.practicum.event.model.Event;
import ru.practicum.event.model.EventAdminFilter;
import ru.practicum.event.model.EventFilter;
import ru.practicum.event.repository.EventRepository;
import ru.practicum.exception.ConditionNotMetException;
import ru.practicum.exception.NoAccessException;
import ru.practicum.exception.NotFoundException;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class EventServiceImpl implements EventService {

    private static final int MIN_TIME_TO_UNPUBLISHED_EVENT = 2;
    private static final int MIN_TIME_TO_PUBLISHED_EVENT = 1;

    private final EventRepository eventRepository;
    private final CategoryService categoryService;
    private final UserClient userClient;
    private final RequestClient requestClient;
    private final CommentClient commentClient;
    private final EventStatsClient eventStatsClient;

    @Override
    @Transactional
    public EventDtoOut add(Long userId, EventCreateDto eventDto) {
        validateEventDate(eventDto.getEventDate(), EventState.PENDING);

        Boolean userExists = userClient.userExists(userId);
        if (userExists == null || !userExists) {
            throw new NotFoundException("User", userId);
        }

        if (!categoryService.categoryExists(eventDto.getCategoryId())) {
            throw new NotFoundException("Category", eventDto.getCategoryId());
        }

        Event event = EventMapper.fromDto(eventDto);
        event.setInitiatorId(userId);
        event.setCategoryId(eventDto.getCategoryId());

        Event saved = eventRepository.save(event);
        return enrichEventWithExternalData(saved);
    }

    @Override
    @Transactional
    public EventDtoOut update(Long userId, Long eventId, EventUpdateDto eventDto) {
        Event event = getEvent(eventId);

        Boolean userExists = userClient.userExists(userId);
        if (userExists == null || !userExists) {
            throw new NotFoundException("User", userId);
        }

        if (!event.getInitiatorId().equals(userId)) {
            throw new NoAccessException("Редактировать событие может только инициатор");
        }

        if (event.getState() == EventState.PUBLISHED) {
            throw new ConditionNotMetException("Не удается обновить опубликованное событие");
        }

        updateEventFields(event, eventDto);

        if (eventDto.getCategoryId() != null
                && !eventDto.getCategoryId().equals(event.getCategoryId())) {
            if (!categoryService.categoryExists(eventDto.getCategoryId())) {
                throw new NotFoundException("Category", eventDto.getCategoryId());
            }
            event.setCategoryId(eventDto.getCategoryId());
        }

        if (eventDto.getEventDate() != null) {
            validateEventDate(eventDto.getEventDate(), event.getState());
            event.setEventDate(eventDto.getEventDate());
        }

        if (eventDto.getStateAction() != null) {
            switch (eventDto.getStateAction()) {
                case SEND_TO_REVIEW -> event.setState(EventState.PENDING);
                case CANCEL_REVIEW -> event.setState(EventState.CANCELED);
            }
        }

        Event updated = eventRepository.save(event);
        return enrichEventWithExternalData(updated);
    }

    private void updateEventFields(Event event, EventUpdateDto eventDto) {
        if (eventDto == null) return;

        if (eventDto.getTitle() != null && !eventDto.getTitle().trim().isEmpty()) {
            event.setTitle(eventDto.getTitle().trim());
        }
        if (eventDto.getAnnotation() != null && !eventDto.getAnnotation().trim().isEmpty()) {
            event.setAnnotation(eventDto.getAnnotation().trim());
        }
        if (eventDto.getDescription() != null && !eventDto.getDescription().trim().isEmpty()) {
            event.setDescription(eventDto.getDescription().trim());
        }
        if (eventDto.getPaid() != null) {
            event.setPaid(eventDto.getPaid());
        }
        if (eventDto.getParticipantLimit() != null) {
            event.setParticipantLimit(eventDto.getParticipantLimit());
        }
        if (eventDto.getRequestModeration() != null) {
            event.setRequestModeration(eventDto.getRequestModeration());
        }
        if (eventDto.getLocation() != null) {
            event.setLocationLat(eventDto.getLocation().getLat());
            event.setLocationLon(eventDto.getLocation().getLon());
        }
    }

    @Override
    @Transactional
    public EventDtoOut update(Long eventId, EventUpdateAdminDto eventDto) {
        Event event = getEvent(eventId);

        updateEventFields(event, eventDto);

        if (eventDto.getCategoryId() != null) {
            if (!categoryService.categoryExists(eventDto.getCategoryId())) {
                throw new NotFoundException("Category", eventDto.getCategoryId());
            }
            event.setCategoryId(eventDto.getCategoryId());
        }

        if (eventDto.getEventDate() != null) {
            validateEventDate(eventDto.getEventDate(), event.getState());
            event.setEventDate(eventDto.getEventDate());
        }

        if (eventDto.getStateAction() != null) {
            switch (eventDto.getStateAction()) {
                case PUBLISH_EVENT -> publishEvent(event);
                case REJECT_EVENT -> rejectEvent(event);
            }
        }

        Event saved = eventRepository.save(event);
        return enrichEventWithExternalData(saved);
    }

    @Override
    public EventDtoOut findPublished(Long eventId) {
        Event event = eventRepository.findPublishedById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));

        Map<Long, Long> viewsMap = eventStatsClient.getViewsForEvents(List.of(eventId));
        event.setViews(viewsMap.getOrDefault(eventId, 0L));

        return enrichEventWithExternalData(event);
    }

    @Override
    public EventDtoOut find(Long userId, Long eventId) {
        Boolean userExists = userClient.userExists(userId);
        if (userExists == null || !userExists) {
            throw new NotFoundException("User", userId);
        }

        Event event = getEvent(eventId);

        if (!event.getInitiatorId().equals(userId)) {
            throw new NoAccessException("Только инициатор может просматривать это событие");
        }
        return enrichEventWithExternalData(event);
    }

    @Override
    public List<EventShortDtoOut> findShortEventsBy(EventFilter filter) {
        Specification<Event> spec = buildSpecification(filter);
        List<Event> events = eventRepository.findAll(spec, filter.getPageable()).getContent();

        enrichEventsWithViews(events);

        return enrichShortEventsWithExternalData(events);
    }


    @Override
    public List<EventDtoOut> findFullEventsBy(EventAdminFilter filter) {
        Specification<Event> spec = buildSpecification(filter);
        List<Event> events = eventRepository.findAll(spec, filter.getPageable()).getContent();

        enrichEventsWithViews(events);

        return enrichEventsWithExternalData(events);
    }

    @Override
    public List<EventShortDtoOut> findByInitiator(Long userId, Integer offset, Integer limit) {
        Boolean userExists = userClient.userExists(userId);
        if (userExists == null || !userExists) {
            throw new NotFoundException("User", userId);
        }

        Pageable pageable = PageRequest.of(offset / limit, limit, Sort.by("id"));
        Page<Event> eventPage = eventRepository.findByInitiatorId(userId, pageable);
        List<Event> events = eventPage.getContent();
        return enrichShortEventsWithExternalData(events);
    }

    @Override
    public EventDtoOut getEventById(Long eventId) {
        Event event = getEvent(eventId);
        return enrichEventWithExternalData(event);
    }

    @Override
    public EventShortDtoOut getShortEventById(Long eventId) {
        Event event = getEvent(eventId);

        Map<Long, Long> viewsMap = eventStatsClient.getViewsForEvents(List.of(eventId));
        event.setViews(viewsMap.getOrDefault(eventId, 0L));

        EventShortDtoOut dto = EventMapper.toShortDto(event);
        dto.setCategory(categoryService.getCategoryById(event.getCategoryId()));
        dto.setInitiator(userClient.getUserById(event.getInitiatorId()));
        dto.setConfirmedRequests(requestClient.getConfirmedRequestsCount(eventId));
        dto.setViews(event.getViews());

        return dto;
    }

    @Override
    public Boolean eventExists(Long eventId) {
        return eventRepository.existsById(eventId);
    }

    @Override
    public List<EventShortDtoOut> getEventsByIds(List<Long> eventIds) {
        List<Event> events = eventRepository.findByIdIn(eventIds);
        return enrichShortEventsWithExternalData(events);
    }

    @Override
    public List<EventDtoOut> getFullEventsByIds(List<Long> eventIds) {
        List<Event> events = eventRepository.findByIdIn(eventIds);
        return enrichEventsWithExternalData(events);
    }

    private Event getEvent(Long eventId) {
        return eventRepository.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));
    }

    private void validateEventDate(LocalDateTime eventDate, EventState state) {
        if (eventDate == null) {
            throw new IllegalArgumentException("Значение EventDate равно нулю");
        }

        int hours = state == EventState.PUBLISHED
                ? MIN_TIME_TO_PUBLISHED_EVENT
                : MIN_TIME_TO_UNPUBLISHED_EVENT;

        if (eventDate.isBefore(LocalDateTime.now().plusHours(hours))) {
            String message = "Дата события должна быть не ранее, чем через %d часов %s"
                    .formatted(hours, state == EventState.PUBLISHED ? "публикации" : "текущего времени");
            throw new ConditionNotMetException(message);
        }
    }

    private void updateEventFields(Event event, EventUpdateAdminDto eventDto) {
        if (eventDto == null) return;

        if (eventDto.getTitle() != null && !eventDto.getTitle().trim().isEmpty()) {
            event.setTitle(eventDto.getTitle().trim());
        }
        if (eventDto.getAnnotation() != null && !eventDto.getAnnotation().trim().isEmpty()) {
            event.setAnnotation(eventDto.getAnnotation().trim());
        }
        if (eventDto.getDescription() != null && !eventDto.getDescription().trim().isEmpty()) {
            event.setDescription(eventDto.getDescription().trim());
        }
        if (eventDto.getPaid() != null) {
            event.setPaid(eventDto.getPaid());
        }
        if (eventDto.getParticipantLimit() != null) {
            event.setParticipantLimit(eventDto.getParticipantLimit());
        }
        if (eventDto.getRequestModeration() != null) {
            event.setRequestModeration(eventDto.getRequestModeration());
        }
        if (eventDto.getLocation() != null) {
            event.setLocationLat(eventDto.getLocation().getLat());
            event.setLocationLon(eventDto.getLocation().getLon());
        }
    }

    private void publishEvent(Event event) {
        if (event.getState() != EventState.PENDING) {
            throw new ConditionNotMetException("Для публикации события должны иметь статус ожидающие");
        }

        validateEventDate(event.getEventDate(), EventState.PUBLISHED);
        event.setState(EventState.PUBLISHED);
        event.setPublishedOn(LocalDateTime.now());
    }

    private void rejectEvent(Event event) {
        if (event.getState() == EventState.PUBLISHED) {
            throw new ConditionNotMetException("Опубликованные события не могут быть отклонены");
        }

        event.setState(EventState.CANCELED);
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

    private List<EventShortDtoOut> enrichShortEventsWithExternalData(List<Event> events) {
        if (events.isEmpty()) {
            return List.of();
        }

        List<Long> categoryIds = events.stream()
                .map(Event::getCategoryId)
                .distinct()
                .toList();

        List<Long> userIds = events.stream()
                .map(Event::getInitiatorId)
                .distinct()
                .toList();

        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .toList();

        Map<Long, CategoryDtoOut> categoriesMap = categoryService.getCategoriesByIds(categoryIds).stream()
                .collect(Collectors.toMap(CategoryDtoOut::getId, c -> c));

        Map<Long, UserDtoOut> usersMap = userClient.getUsersByIds(userIds).stream()
                .collect(Collectors.toMap(UserDtoOut::getId, u -> u));

        Map<Long, Integer> confirmedRequestsMap = requestClient.getConfirmedRequestsCounts(eventIds);

        return events.stream()
                .map(event -> {
                    EventShortDtoOut dto = EventMapper.toShortDto(event);
                    dto.setCategory(categoriesMap.get(event.getCategoryId()));
                    dto.setInitiator(usersMap.get(event.getInitiatorId()));
                    dto.setConfirmedRequests(confirmedRequestsMap.getOrDefault(event.getId(), 0));
                    dto.setViews(event.getViews());
                    return dto;
                })
                .toList();
    }

    private void enrichEventsWithViews(List<Event> events) {
        if (events.isEmpty()) {
            return;
        }

        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .collect(Collectors.toList());

        Map<Long, Long> eventViewsMap = eventStatsClient.getViewsForEvents(eventIds);

        events.forEach(event ->
                event.setViews(eventViewsMap.getOrDefault(event.getId(), 0L))
        );
    }

    private EventDtoOut enrichEventWithExternalData(Event event) {
        Map<Long, Long> viewsMap = eventStatsClient.getViewsForEvents(List.of(event.getId()));
        event.setViews(viewsMap.getOrDefault(event.getId(), 0L));

        EventDtoOut dto = EventMapper.toDto(event);
        dto.setCategory(categoryService.getCategoryById(event.getCategoryId()));
        dto.setInitiator(userClient.getUserById(event.getInitiatorId()));
        dto.setConfirmedRequests(getSafeConfirmedRequestsCount(event.getId()));
        dto.setViews(event.getViews());

        return dto;
    }

    private List<EventDtoOut> enrichEventsWithExternalData(List<Event> events) {
        if (events.isEmpty()) {
            return List.of();
        }

        List<Long> categoryIds = events.stream()
                .map(Event::getCategoryId)
                .distinct()
                .toList();

        List<Long> userIds = events.stream()
                .map(Event::getInitiatorId)
                .distinct()
                .toList();

        List<Long> eventIds = events.stream()
                .map(Event::getId)
                .toList();

        Map<Long, CategoryDtoOut> categoriesMap = categoryService.getCategoriesByIds(categoryIds).stream()
                .collect(Collectors.toMap(CategoryDtoOut::getId, c -> c));

        Map<Long, UserDtoOut> usersMap = userClient.getUsersByIds(userIds).stream()
                .collect(Collectors.toMap(UserDtoOut::getId, u -> u));

        Map<Long, Integer> confirmedRequestsMap = getSafeConfirmedRequestsCounts(eventIds);

        return events.stream()
                .map(event -> {
                    EventDtoOut dto = EventMapper.toDto(event);
                    dto.setCategory(categoriesMap.get(event.getCategoryId()));
                    dto.setInitiator(usersMap.get(event.getInitiatorId()));
                    dto.setConfirmedRequests(confirmedRequestsMap.getOrDefault(event.getId(), 0));
                    dto.setViews(event.getViews());
                    return dto;
                })
                .toList();
    }

    // Безопасные методы для возврата default значений
    private Integer getSafeConfirmedRequestsCount(Long eventId) {
        try {
            return requestClient.getConfirmedRequestsCount(eventId);
        } catch (Exception e) {
            log.warn("Request service unavailable for event {}, returning 0: {}", eventId, e.getMessage());
            return 0; //  возвращаем 0 при недоступности
        }
    }

    private Map<Long, Integer> getSafeConfirmedRequestsCounts(List<Long> eventIds) {
        try {
            return requestClient.getConfirmedRequestsCounts(eventIds);
        } catch (Exception e) {
            log.warn("Request service unavailable, returning 0 for all events: {}", e.getMessage());
            // возвращаем 0 для всех событий
            return eventIds.stream().collect(java.util.stream.Collectors.toMap(id -> id, id -> 0));
        }
    }

    private Long getSafeCommentsCount(Long eventId) {
        try {
            Long count = commentClient.getCountPublishedCommentsByEventId(eventId);
            return count != null ? count : 0L;
        } catch (Exception e) {
            log.warn("Comment service unavailable for event {}, returning 0: {}", eventId, e.getMessage());
            return 0L; // возвращаем 0 комментариев
        }
    }
}