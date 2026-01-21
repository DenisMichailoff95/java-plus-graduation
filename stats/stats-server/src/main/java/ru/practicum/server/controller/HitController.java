package ru.practicum.server.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.practicum.dto.EndpointHitDTO;
import ru.practicum.dto.ViewStatsDTO;
import ru.practicum.server.service.HitService;

import java.time.LocalDateTime;
import java.util.List;

@Validated
@RestController
@RequiredArgsConstructor
@Slf4j
public class HitController {

    private final HitService hitService;

    @PostMapping("/hit")
    @ResponseStatus(HttpStatus.CREATED)
    public void createHit(@RequestBody @Valid EndpointHitDTO endpointHitDTO) {
        log.info("Received hit: app={}, uri={}, ip={}",
                endpointHitDTO.getApp(), endpointHitDTO.getUri(), endpointHitDTO.getIp());

        hitService.createHit(endpointHitDTO);
    }

    @PostMapping("/hit/async")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void createHitAsync(@RequestBody @Valid EndpointHitDTO endpointHitDTO) {
        log.debug("Received async hit: {}", endpointHitDTO);
        hitService.createHitAsync(endpointHitDTO);
    }

    @PostMapping("/hit/batch")
    @ResponseStatus(HttpStatus.CREATED)
    public void createHitsBatch(@RequestBody @Valid List<EndpointHitDTO> hits) {
        log.info("Received batch of {} hits", hits.size());

        for (EndpointHitDTO hit : hits) {
            try {
                hitService.createHit(hit);
            } catch (Exception e) {
                log.warn("Failed to save hit in batch: {}", e.getMessage());
                // Продолжаем обработку остальных хитов
            }
        }
    }

    @GetMapping("/stats")
    @ResponseStatus(HttpStatus.OK)
    public List<ViewStatsDTO> getStats(
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime start,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime end,
            @RequestParam(required = false) List<String> uris,
            @RequestParam(defaultValue = "false") Boolean unique) {

        log.info("Stats request: start={}, end={}, uris={}, unique={}", start, end, uris, unique);

        return hitService.getStats(start, end, uris, unique);
    }

    @GetMapping("/stats/total")
    @ResponseStatus(HttpStatus.OK)
    public Long getTotalHits(
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime start,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime end) {

        log.debug("Total hits request: start={}, end={}", start, end);

        return hitService.getTotalHits(start, end);
    }

    @GetMapping("/stats/unique")
    @ResponseStatus(HttpStatus.OK)
    public Long getUniqueHits(
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime start,
            @RequestParam @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime end) {

        log.debug("Unique hits request: start={}, end={}", start, end);

        return hitService.getUniqueHits(start, end);
    }

    @DeleteMapping("/stats/cleanup")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void cleanupOldHits(@RequestParam(defaultValue = "365") int daysToKeep) {
        log.info("Cleanup request for hits older than {} days", daysToKeep);

        hitService.cleanupOldHits(daysToKeep);
    }

    @GetMapping("/health")
    @ResponseStatus(HttpStatus.OK)
    public String health() {
        return "OK";
    }

    @GetMapping("/info")
    @ResponseStatus(HttpStatus.OK)
    public String info() {
        return "Stats Service v1.0.0";
    }
}