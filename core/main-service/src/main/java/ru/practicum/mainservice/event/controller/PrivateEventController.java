package ru.practicum.mainservice.event.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.mainservice.event.dto.EventCreateDto;
import ru.practicum.mainservice.event.dto.EventDtoOut;
import ru.practicum.mainservice.event.dto.EventShortDtoOut;
import ru.practicum.mainservice.event.dto.EventUpdateDto;
import ru.practicum.mainservice.event.service.EventService;

import java.util.Collection;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/users")
public class PrivateEventController {

    private final EventService eventService;

    @GetMapping("/{userId}/events")
    public Collection<EventShortDtoOut> getEventsCreatedByUser(
            @PathVariable @Min(1) Long userId,
            @RequestParam(name = "from", defaultValue = "0") @Min(0) Integer offset,
            @RequestParam(name = "size", defaultValue = "10") @Min(1) Integer limit) {

        log.info("запрос: получение всех событий, созданных по идентификатору пользователя:{}", userId);

        return eventService.findByInitiator(userId, offset, limit);
    }

    @PostMapping("/{userId}/events")
    @ResponseStatus(HttpStatus.CREATED)
    public EventDtoOut createEvent(@PathVariable @Min(1) Long userId,
                                   @RequestBody @Valid EventCreateDto eventDto) {
        log.info("запрос : создать новое событие: {}", eventDto);
        return eventService.add(userId, eventDto);
    }

    @PatchMapping("/{userId}/events/{eventId}")
    public EventDtoOut updateEvent(
            @PathVariable @Min(1) Long userId,
            @PathVariable @Min(1) Long eventId,
            @RequestBody @Valid EventUpdateDto eventDto) {
        log.info("запрос : событие обновления: {}", eventDto);
        return eventService.update(userId, eventId, eventDto);
    }

    @GetMapping("/{userId}/events/{eventId}")
    public EventDtoOut getEventById(@PathVariable @Min(1) Long userId,
                                    @PathVariable @Min(1) Long eventId) {
        log.info("запрос : получить событие: {}", eventId);
        return eventService.find(userId, eventId);
    }
}
