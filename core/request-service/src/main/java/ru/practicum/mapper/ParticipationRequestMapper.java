package ru.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.practicum.dto.participation.ParticipationRequestDto;
import ru.practicum.model.ParticipationRequest;

@Mapper(componentModel = "spring")
public interface ParticipationRequestMapper {

    @Mapping(source = "created", target = "created")
    @Mapping(source = "eventId", target = "event")
    @Mapping(source = "requesterId", target = "requester")
    @Mapping(expression = "java(request.getStatus() != null ? request.getStatus().name() : \"UNKNOWN\")",
            target = "status")
    ParticipationRequestDto toDto(ParticipationRequest request);
}