package ru.practicum.event.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.practicum.dto.event.EventCreateDto;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.event.LocationDto;
import ru.practicum.event.model.Event;

@Mapper(componentModel = "spring")
public interface EventMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "categoryId", ignore = true)
    @Mapping(target = "initiatorId", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "publishedOn", ignore = true)
    @Mapping(target = "state", ignore = true)
    @Mapping(target = "confirmedRequests", ignore = true)
    @Mapping(target = "rating", ignore = true)
    @Mapping(source = "location.lat", target = "locationLat")
    @Mapping(source = "location.lon", target = "locationLon")
    @Mapping(target = "paid", defaultValue = "false")
    @Mapping(target = "participantLimit", defaultValue = "0")
    @Mapping(target = "requestModeration", defaultValue = "true")
    Event fromDto(EventCreateDto eventDto);

    @Mapping(target = "category", ignore = true)
    @Mapping(target = "initiator", ignore = true)
    @Mapping(target = "location", expression = "java(mapToLocationDto(event))")
    @Mapping(target = "state", expression = "java(event.getState() != null ? event.getState().name() : null)")
    @Mapping(target = "confirmedRequests", ignore = true)
    @Mapping(target = "rating", source = "rating")
    EventDtoOut toDto(Event event);

    @Mapping(target = "category", ignore = true)
    @Mapping(target = "initiator", ignore = true)
    @Mapping(target = "confirmedRequests", ignore = true)
    @Mapping(target = "rating", source = "rating")
    EventShortDtoOut toShortDto(Event event);

    default LocationDto mapToLocationDto(Event event) {
        if (event.getLocationLat() == null || event.getLocationLon() == null) {
            return null;
        }
        return new LocationDto(event.getLocationLat(), event.getLocationLon());
    }
}