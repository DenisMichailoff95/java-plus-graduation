package ru.practicum.event.mapper;

import lombok.experimental.UtilityClass;
import ru.practicum.dto.event.EventCreateDto;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.event.LocationDto;
import ru.practicum.event.model.Event;

@UtilityClass
public class EventMapper {
    public static Event fromDto(EventCreateDto eventDto) {
        return Event.builder()
                .annotation(eventDto.getAnnotation())
                .title(eventDto.getTitle())
                .paid(eventDto.getPaid())
                .eventDate(eventDto.getEventDate())
                .description(eventDto.getDescription())
                .locationLat(eventDto.getLocation().getLat())
                .locationLon(eventDto.getLocation().getLon())
                .participantLimit(eventDto.getParticipantLimit())
                .requestModeration(eventDto.getRequestModeration())
                .build();
    }

    public static EventDtoOut toDto(Event event) {
        return EventDtoOut.builder()
                .id(event.getId())
                .annotation(event.getAnnotation())
                .title(event.getTitle())
                .paid(event.getPaid())
                .eventDate(event.getEventDate())
                .description(event.getDescription())
                .createdOn(event.getCreatedAt())
                .state(event.getState().name())
                .confirmedRequests(event.getConfirmedRequests())
                .views(event.getViews())
                .location(new LocationDto(event.getLocationLat(), event.getLocationLon()))
                .participantLimit(event.getParticipantLimit())
                .requestModeration(event.getRequestModeration())
                .build();
    }

    public static EventShortDtoOut toShortDto(Event event) {
        return EventShortDtoOut.builder()
                .id(event.getId())
                .annotation(event.getAnnotation())
                .title(event.getTitle())
                .paid(event.getPaid())
                .eventDate(event.getEventDate())
                .confirmedRequests(event.getConfirmedRequests())
                .views(event.getViews())
                .build();
    }
}