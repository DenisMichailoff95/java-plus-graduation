package ru.practicum.mainservice.comment.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.mainservice.comment.dto.CommentCreateDto;
import ru.practicum.mainservice.comment.dto.CommentDto;
import ru.practicum.mainservice.comment.dto.CommentUpdateDto;
import ru.practicum.mainservice.comment.service.CommentService;

import java.util.List;

@RestController
@RequestMapping("/users/{userId}")
@RequiredArgsConstructor
@Validated
public class CommentPrivateController {

    private final CommentService commentService;

    @PostMapping("/events/{eventId}/comments")
    @ResponseStatus(HttpStatus.CREATED)
    public CommentDto createComment(@PathVariable @Min(1) Long userId,
                                    @PathVariable @Min(1) Long eventId,
                                    @RequestBody @Valid CommentCreateDto commentCreateDto) {
        return commentService.createComment(userId, eventId, commentCreateDto);
    }

    @PatchMapping("/comments/{commentId}")
    public CommentDto updateComment(@PathVariable @Min(1) Long userId,
                                    @PathVariable @Min(1) Long commentId,
                                    @RequestBody @Valid CommentUpdateDto commentUpdateDto) {
        return commentService.updateComment(userId, commentId, commentUpdateDto);
    }

    @DeleteMapping("/comments/{commentId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteComment(@PathVariable @Min(1) Long userId,
                              @PathVariable @Min(1) Long commentId) {
        commentService.deleteCommentByUser(userId, commentId);
    }

    @GetMapping("/comments")
    public List<CommentDto> getUserComments(@PathVariable @Min(1) Long userId,
                                            @RequestParam(defaultValue = "0") @Min(0) Integer from,
                                            @RequestParam(defaultValue = "10") @Min(1) Integer size) {
        Pageable pageable = PageRequest.of(from / size, size);
        return commentService.getUserComments(userId, pageable);
    }
}
