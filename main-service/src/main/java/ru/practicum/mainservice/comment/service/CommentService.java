package ru.practicum.mainservice.comment.service;

import ru.practicum.mainservice.comment.dto.CommentCreateDto;
import ru.practicum.mainservice.comment.dto.CommentDto;
import ru.practicum.mainservice.comment.dto.CommentUpdateDto;
import org.springframework.data.domain.Pageable;

import java.util.List;

public interface CommentService {

    CommentDto createComment(Long userId, Long eventId, CommentCreateDto commentCreateDto);

    CommentDto updateComment(Long userId, Long commentId, CommentUpdateDto commentUpdateDto);

    void deleteCommentByUser(Long userId, Long commentId);

    void deleteCommentByAdmin(Long commentId);

    CommentDto getComment(Long commentId);

    List<CommentDto> getEventComments(Long eventId, Pageable pageable);

    List<CommentDto> getUserComments(Long userId, Pageable pageable);

    List<CommentDto> getCommentsAdmin(List<Long> events, List<Long> users, Pageable pageable);
}
