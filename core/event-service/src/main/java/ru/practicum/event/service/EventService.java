package ru.practicum.event.service;

import ru.practicum.dto.event.EventCreateDto;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.event.EventUpdateAdminDto;
import ru.practicum.dto.event.EventUpdateDto;
import ru.practicum.event.model.EventAdminFilter;
import ru.practicum.event.model.EventFilter;

import java.util.List;

public interface EventService {
    EventDtoOut add(Long userId, EventCreateDto eventDto);

    EventDtoOut update(Long userId, Long eventId, EventUpdateDto updateRequest);

    EventDtoOut update(Long eventId, EventUpdateAdminDto eventDto);

    EventDtoOut findPublished(Long eventId);

    EventDtoOut find(Long userId, Long eventId);

    List<EventShortDtoOut> findShortEventsBy(EventFilter filter);

    List<EventDtoOut> findFullEventsBy(EventAdminFilter filter);

    List<EventShortDtoOut> findByInitiator(Long userId, Integer offset, Integer limit);

    // Новые методы для рекомендаций
    List<EventShortDtoOut> getRecommendationsForUser(Long userId, int maxResults);

    void sendLikeAction(Long userId, Long eventId);

    // Методы для внутреннего использования
    EventDtoOut getEventById(Long eventId);

    EventShortDtoOut getShortEventById(Long eventId);

    Boolean eventExists(Long eventId);

    List<EventShortDtoOut> getEventsByIds(List<Long> eventIds);

    List<EventDtoOut> getFullEventsByIds(List<Long> eventIds);

    EventDtoOut findPublishedWithUser(Long eventId, Long userId);
}