package ru.practicum.event.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.client.EventClient;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.event.service.EventService;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/events")
public class EventClientController implements EventClient {

    private final EventService eventService;

    @Override
    @GetMapping("/{eventId}")
    public EventDtoOut getEventById(@PathVariable Long eventId) {
        try {
            return eventService.getEventById(eventId);
        } catch (Exception e) {
            log.error("Error in getEventById: eventId={}, error={}", eventId, e.getMessage());
            throw e;
        }
    }

    @Override
    @GetMapping("/short/{eventId}")
    public EventShortDtoOut getShortEventById(@PathVariable Long eventId) {
        try {
            return eventService.getShortEventById(eventId);
        } catch (Exception e) {
            log.error("Error in getShortEventById: eventId={}, error={}", eventId, e.getMessage());
            throw e;
        }
    }

    @Override
    @GetMapping("/exists/{eventId}")
    public Boolean eventExists(@PathVariable Long eventId) {
        try {
            return eventService.eventExists(eventId);
        } catch (Exception e) {
            log.error("Error in eventExists: eventId={}, error={}", eventId, e.getMessage());
            return false;
        }
    }

    @Override
    @GetMapping("/batch")
    public List<EventShortDtoOut> getEventsByIds(@RequestParam List<Long> eventIds) {
        try {
            return eventService.getEventsByIds(eventIds);
        } catch (Exception e) {
            log.error("Error in getEventsByIds: eventIds={}, error={}", eventIds, e.getMessage());
            return List.of();
        }
    }

    @Override
    @GetMapping("/batch/full")
    public List<EventDtoOut> getFullEventsByIds(@RequestParam List<Long> eventIds) {
        try {
            return eventService.getFullEventsByIds(eventIds);
        } catch (Exception e) {
            log.error("Error in getFullEventsByIds: eventIds={}, error={}", eventIds, e.getMessage());
            return List.of();
        }
    }

    @Override
    @PutMapping("/{eventId}/increment")
    public void incrementConfirmedRequests(@PathVariable Long eventId,
                                           @RequestParam(required = false, defaultValue = "1") int count) {
        log.info("Increment confirmed requests for event {} by {}", eventId, count);
    }
}