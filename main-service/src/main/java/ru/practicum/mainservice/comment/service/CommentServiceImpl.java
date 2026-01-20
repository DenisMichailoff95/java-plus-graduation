package ru.practicum.mainservice.comment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.mainservice.comment.dto.CommentCreateDto;
import ru.practicum.mainservice.comment.dto.CommentDto;
import ru.practicum.mainservice.comment.dto.CommentUpdateDto;
import ru.practicum.mainservice.comment.mapper.CommentMapper;
import ru.practicum.mainservice.comment.model.Comment;
import ru.practicum.mainservice.comment.model.CommentStatus;
import ru.practicum.mainservice.comment.repository.CommentRepository;
import ru.practicum.mainservice.event.model.Event;
import ru.practicum.mainservice.event.model.EventState;
import ru.practicum.mainservice.event.repository.EventRepository;
import ru.practicum.mainservice.exception.ConditionNotMetException;
import ru.practicum.mainservice.exception.NoAccessException;
import ru.practicum.mainservice.exception.NotFoundException;
import ru.practicum.mainservice.user.model.User;
import ru.practicum.mainservice.user.repository.UserRepository;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CommentServiceImpl implements CommentService {

    private final CommentRepository commentRepository;
    private final UserRepository userRepository;
    private final EventRepository eventRepository;

    @Override
    @Transactional
    public CommentDto createComment(Long userId, Long eventId, CommentCreateDto commentCreateDto) {
        log.info("Создание комментария пользователем {} к событию {}", userId, eventId);

        User user = userRepository.findById(userId)
                .orElseThrow(() -> new NotFoundException("User", userId));

        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new NotFoundException("Event", eventId));

        if (event.getState() != EventState.PUBLISHED) {
            throw new ConditionNotMetException("Нельзя оставлять комментарии к неопубликованному событию");
        }

        Comment comment = Comment.builder()
                .text(commentCreateDto.getText().trim())
                .user(user)
                .event(event)
                .status(CommentStatus.PUBLISHED)
                .build();

        Comment saved = commentRepository.save(comment);
        log.info("Создан комментарий ID: {} пользователем ID: {} к событию ID: {}",
                saved.getId(), userId, eventId);
        return CommentMapper.toDto(saved);
    }

    @Override
    @Transactional
    public CommentDto updateComment(Long userId, Long commentId, CommentUpdateDto commentUpdateDto) {
        log.info("Обновление комментария {} пользователем {}", commentId, userId);

        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Comment", commentId));

        if (!comment.getUser().getId().equals(userId)) {
            throw new NoAccessException("Редактировать можно только свои комментарии");
        }

        if (comment.getStatus() == CommentStatus.DELETED) {
            throw new ConditionNotMetException("Нельзя редактировать удаленный комментарий");
        }

        if (commentUpdateDto.getText() != null && !commentUpdateDto.getText().trim().isEmpty()) {
            comment.setText(commentUpdateDto.getText().trim());
            comment.setStatus(CommentStatus.EDITED);
        }

        Comment updated = commentRepository.save(comment);
        return CommentMapper.toDto(updated);
    }

    @Override
    @Transactional
    public void deleteCommentByUser(Long userId, Long commentId) {
        log.info("Удаление комментария {} пользователем {}", commentId, userId);

        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Comment", commentId));

        if (!comment.getUser().getId().equals(userId)) {
            throw new NoAccessException("Удалять можно только свои комментарии");
        }

        comment.setStatus(CommentStatus.DELETED);
        commentRepository.save(comment);
    }

    @Override
    @Transactional
    public void deleteCommentByAdmin(Long commentId) {
        log.info("Удаление комментария {} администратором", commentId);

        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Comment", commentId));

        comment.setStatus(CommentStatus.DELETED);
        commentRepository.save(comment);
    }

    @Override
    public CommentDto getComment(Long commentId) {
        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Comment", commentId));

        if (comment.getStatus() == CommentStatus.DELETED) {
            throw new NotFoundException("Comment", commentId);
        }

        return CommentMapper.toDto(comment);
    }

    @Override
    public List<CommentDto> getEventComments(Long eventId, Pageable pageable) {
        log.info("Получение комментариев события {}", eventId);

        if (!eventRepository.existsById(eventId)) {
            throw new NotFoundException("Event", eventId);
        }

        List<CommentStatus> activeStatuses = List.of(CommentStatus.PUBLISHED, CommentStatus.EDITED);
        return commentRepository
                .findByEventIdAndStatusInOrderByCreatedAtDesc(eventId, activeStatuses, pageable)
                .getContent()
                .stream()
                .map(CommentMapper::toDto)
                .toList();
    }

    @Override
    public List<CommentDto> getUserComments(Long userId, Pageable pageable) {
        log.info("Получение комментариев пользователя {}", userId);

        if (!userRepository.existsById(userId)) {
            throw new NotFoundException("User", userId);
        }

        return commentRepository
                .findByUserIdAndStatusNotOrderByCreatedAtDesc(userId, CommentStatus.DELETED, pageable)
                .getContent()
                .stream()
                .map(CommentMapper::toDto)
                .toList();
    }

    @Override
    public List<CommentDto> getCommentsAdmin(List<Long> events, List<Long> users, Pageable pageable) {
        log.info("Получение комментариев админом events: {}, users: {}", events, users);

        return commentRepository
                .findByEventIdInAndUserIdInOrderByCreatedAtDesc(events, users, pageable)
                .getContent()
                .stream()
                .map(CommentMapper::toDto)
                .toList();
    }
}