package ru.practicum.event.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventUpdateAdminDto;
import ru.practicum.enums.EventState;
import ru.practicum.event.model.EventAdminFilter;
import ru.practicum.event.service.EventService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static ru.practicum.constants.Constants.DATE_TIME_FORMAT;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/admin/events")
public class AdminEventController {

    private final EventService eventService;

    @GetMapping
    public List<EventDtoOut> getEvents(
            @RequestParam(required = false) List<Long> users,
            @RequestParam(required = false) List<Long> categories,
            @RequestParam(required = false) List<String> states,
            @RequestParam(required = false) @DateTimeFormat(pattern = DATE_TIME_FORMAT) LocalDateTime rangeStart,
            @RequestParam(required = false) @DateTimeFormat(pattern = DATE_TIME_FORMAT) LocalDateTime rangeEnd,
            @RequestParam(defaultValue = "0") Integer from,
            @RequestParam(defaultValue = "10") Integer size) {

        log.info("request from Admin: get all events. Params: users={}, categories={}, states={}, rangeStart={}, rangeEnd={}",
                users, categories, states, rangeStart, rangeEnd);

        List<EventState> eventStates = null;
        if (states != null && !states.isEmpty()) {
            try {
                eventStates = states.stream()
                        .map(String::toUpperCase)
                        .map(EventState::valueOf)
                        .collect(Collectors.toList());
            } catch (IllegalArgumentException e) {
                log.warn("Invalid event state provided: {}", states);
            }
        }

        EventAdminFilter filter = EventAdminFilter.builder()
                .users(users)
                .categories(categories)
                .states(eventStates)
                .rangeStart(rangeStart)
                .rangeEnd(rangeEnd)
                .from(from)
                .size(size)
                .build();

        log.debug("Filter created: {}", filter);

        List<EventDtoOut> result = eventService.findFullEventsBy(filter);
        log.info("Found {} events", result.size());

        return result;
    }

    @PatchMapping("/{eventId}")
    public EventDtoOut updateEvent(
            @PathVariable @Min(1) Long eventId,
            @RequestBody @Valid EventUpdateAdminDto eventDto) {
        log.info("request from Admin: update event:{}", eventId);
        return eventService.update(eventId, eventDto);
    }
}