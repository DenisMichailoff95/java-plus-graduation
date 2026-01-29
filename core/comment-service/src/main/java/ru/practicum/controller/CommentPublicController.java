package ru.practicum.controller;

import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.validation.annotation.Validated;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.dto.comment.CommentDto;
import ru.practicum.service.CommentService;

import java.util.List;

@RestController
@RequestMapping("/events/{eventId}/comments")
@RequiredArgsConstructor
@Validated
public class CommentPublicController {

    private final CommentService commentService;

    @GetMapping
    public List<CommentDto> getEventComments(
            @PathVariable @Min(1) Long eventId,
            @RequestParam(defaultValue = "0") @Min(0) Integer from,
            @RequestParam(defaultValue = "10") @Min(1) Integer size) {

        Pageable pageable = PageRequest.of(from / size, size);
        return commentService.getEventComments(eventId, pageable);
    }

    @GetMapping("/{commentId}")
    public CommentDto getComment(@PathVariable @Min(1) Long eventId,
                                 @PathVariable @Min(1) Long commentId) {
        return commentService.getComment(commentId);
    }
}