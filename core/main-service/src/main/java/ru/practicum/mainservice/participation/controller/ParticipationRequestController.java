package ru.practicum.mainservice.participation.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.mainservice.exception.ConditionNotMetException;
import ru.practicum.mainservice.exception.ForbiddenException;
import ru.practicum.mainservice.exception.NoAccessException;
import ru.practicum.mainservice.exception.NotFoundException;
import ru.practicum.mainservice.participation.dto.ParticipationRequestDto;
import ru.practicum.mainservice.participation.service.ParticipationRequestService;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@Validated
public class ParticipationRequestController {

    private final ParticipationRequestService requestService;

    @PostMapping("/users/{userId}/requests")
    public ResponseEntity<ParticipationRequestDto> createRequest(
            @PathVariable Long userId,
            @RequestParam Long eventId) {

        log.info("Creating participation request for user {} to event {}", userId, eventId);

        try {
            ParticipationRequestDto createdRequest = requestService.createRequest(userId, eventId);
            log.info("Participation request created successfully: {}", createdRequest);
            return new ResponseEntity<>(createdRequest, HttpStatus.CREATED);
        } catch (ConditionNotMetException e) {
            log.warn("Conflict creating participation request: {}", e.getMessage());
            return new ResponseEntity<>(HttpStatus.CONFLICT);
        } catch (NotFoundException e) {
            log.warn("Not found creating participation request: {}", e.getMessage());
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } catch (ForbiddenException | NoAccessException e) {
            log.warn("Forbidden creating participation request: {}", e.getMessage());
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        } catch (Exception e) {
            log.error("Error creating participation request", e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @GetMapping("/users/{userId}/requests")
    public ResponseEntity<List<ParticipationRequestDto>> getUserRequests(@PathVariable Long userId) {
        try {
            List<ParticipationRequestDto> requests = requestService.getUserRequests(userId);
            return ResponseEntity.ok(requests);
        } catch (NotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @PatchMapping("/users/{userId}/requests/{requestId}/cancel")
    public ResponseEntity<ParticipationRequestDto> cancelRequest(
            @PathVariable Long userId,
            @PathVariable Long requestId) {
        try {
            ParticipationRequestDto canceledRequest = requestService.cancelRequest(userId, requestId);
            return ResponseEntity.ok(canceledRequest);
        } catch (NotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        } catch (ForbiddenException e) {
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        } catch (ConditionNotMetException e) {
            return new ResponseEntity<>(HttpStatus.CONFLICT);
        }
    }
}