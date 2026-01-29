package ru.practicum.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "comment-service", contextId = "commentClient", path = "/internal/comments")
@CircuitBreaker(name = "comment-service")
@Retry(name = "comment-service")
public interface CommentClient {

    @GetMapping("/count/{eventId}")
    Long getCountPublishedCommentsByEventId(@PathVariable Long eventId);

    @GetMapping("/exists/{commentId}")
    Boolean commentExists(@PathVariable Long commentId);
}