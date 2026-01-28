package ru.practicum.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.client.CommentClient;
import ru.practicum.service.CommentService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/comments")
public class CommentClientController implements CommentClient {
    private final CommentService commentService;

    @Override
    @GetMapping("/count/{eventId}")
    public Long getCountPublishedCommentsByEventId(@PathVariable Long eventId) {
        return commentService.getCountPublishedCommentsByEventId(eventId);
    }

    @Override
    @GetMapping("/exists/{commentId}")
    public Boolean commentExists(@PathVariable Long commentId) {
        return commentService.commentExists(commentId);
    }
}