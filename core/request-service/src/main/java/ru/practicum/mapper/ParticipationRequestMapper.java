package ru.practicum.mapper;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.practicum.dto.participation.ParticipationRequestDto;
import ru.practicum.model.ParticipationRequest;

import java.time.LocalDateTime;

@Slf4j
@UtilityClass
public class ParticipationRequestMapper {
    public static ParticipationRequestDto toDto(ParticipationRequest request) {
        if (request == null) {
            log.error("Cannot map null ParticipationRequest to DTO");
            return null;
        }

        LocalDateTime created = request.getCreated();

        if (created == null) {
            log.warn("Request id={} has null created field, using current time", request.getId());
            created = LocalDateTime.now();
        }

        ParticipationRequestDto dto = ParticipationRequestDto.builder()
                .id(request.getId())
                .created(created)
                .event(request.getEventId())
                .requester(request.getRequesterId())
                .status(request.getStatus() != null ? request.getStatus().name() : "UNKNOWN")
                .build();

        log.debug("Mapped request id={} to DTO: created={}, event={}, requester={}",
                request.getId(), dto.getCreated(), dto.getEvent(), dto.getRequester());

        return dto;
    }
}