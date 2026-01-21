package ru.practicum.mainservice.participation.mapper;

import lombok.experimental.UtilityClass;
import ru.practicum.mainservice.participation.dto.ParticipationRequestDto;
import ru.practicum.mainservice.participation.model.ParticipationRequest;

@UtilityClass
public class ParticipationRequestMapper {
    public static ParticipationRequestDto toDto(ParticipationRequest r) {
        return ParticipationRequestDto.builder()
                .id(r.getId())
                .created(r.getCreated())
                .event(r.getEvent().getId())
                .requester(r.getRequester().getId())
                .status(r.getStatus().name())
                .build();
    }
}