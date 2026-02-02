package ru.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.client.EventClient;
import ru.practicum.client.UserClient;
import ru.practicum.dto.comment.CommentCreateDto;
import ru.practicum.dto.comment.CommentDto;
import ru.practicum.dto.comment.CommentUpdateDto;
import ru.practicum.dto.event.EventDtoOut;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.enums.CommentStatus;
import ru.practicum.exception.ConditionNotMetException;
import ru.practicum.exception.NoAccessException;
import ru.practicum.exception.NotFoundException;
import ru.practicum.mapper.CommentMapper;
import ru.practicum.model.Comment;
import ru.practicum.repository.CommentRepository;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CommentServiceImpl implements CommentService {

    private final CommentRepository commentRepository;
    private final UserClient userClient;
    private final EventClient eventClient;
    private final CommentMapper commentMapper;

    @Override
    @Transactional
    public CommentDto createComment(Long userId, Long eventId, CommentCreateDto commentCreateDto) {
        log.info("Создание комментария пользователем {} к событию {}", userId, eventId);

        Boolean userExists = userClient.userExists(userId);
        if (userExists == null || !userExists) {
            throw new NotFoundException("User", userId);
        }

        EventDtoOut event = eventClient.getEventById(eventId);
        if (event == null) {
            throw new NotFoundException("Event", eventId);
        }

        if (!"PUBLISHED".equals(event.getState())) {
            throw new ConditionNotMetException("Нельзя оставлять комментарии к неопубликованному событию");
        }

        Comment comment = Comment.builder()
                .text(commentCreateDto.getText().trim())
                .userId(userId)
                .eventId(eventId)
                .status(CommentStatus.PUBLISHED)
                .build();

        Comment saved = commentRepository.save(comment);

        UserDtoOut author = userClient.getUserById(userId);
        EventShortDtoOut eventShort = eventClient.getShortEventById(eventId);

        log.info("Создан комментарий ID: {} пользователем ID: {} к событию ID: {}",
                saved.getId(), userId, eventId);

        return commentMapper.toDto(saved, eventShort, author);
    }

    @Override
    @Transactional
    public CommentDto updateComment(Long userId, Long commentId, CommentUpdateDto commentUpdateDto) {
        log.info("Обновление комментария {} пользователем {}", commentId, userId);

        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Comment", commentId));

        if (!comment.getUserId().equals(userId)) {
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

        EventShortDtoOut event = eventClient.getShortEventById(comment.getEventId());
        UserDtoOut author = userClient.getUserById(userId);

        return commentMapper.toDto(updated, event, author);
    }

    @Override
    @Transactional
    public void deleteCommentByUser(Long userId, Long commentId) {
        log.info("Удаление комментария {} пользователем {}", commentId, userId);

        Comment comment = commentRepository.findById(commentId)
                .orElseThrow(() -> new NotFoundException("Comment", commentId));

        if (!comment.getUserId().equals(userId)) {
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

        EventShortDtoOut event = eventClient.getShortEventById(comment.getEventId());
        UserDtoOut author = userClient.getUserById(comment.getUserId());

        return commentMapper.toDto(comment, event, author);
    }

    @Override
    public List<CommentDto> getEventComments(Long eventId, Pageable pageable) {
        log.info("Получение комментариев события {}", eventId);

        Boolean eventExists = eventClient.eventExists(eventId);
        if (eventExists == null || !eventExists) {
            throw new NotFoundException("Event", eventId);
        }

        List<CommentStatus> activeStatuses = List.of(CommentStatus.PUBLISHED, CommentStatus.EDITED);
        Page<Comment> commentsPage = commentRepository
                .findByEventIdAndStatusInOrderByCreatedAtDesc(eventId, activeStatuses, pageable);

        List<Comment> comments = commentsPage.getContent();

        return enrichCommentsWithExternalData(comments);
    }

    @Override
    public List<CommentDto> getUserComments(Long userId, Pageable pageable) {
        log.info("Получение комментариев пользователя {}", userId);

        Boolean userExists = userClient.userExists(userId);
        if (userExists == null || !userExists) {
            throw new NotFoundException("User", userId);
        }

        Page<Comment> commentsPage = commentRepository
                .findByUserIdAndStatusNotOrderByCreatedAtDesc(userId, CommentStatus.DELETED, pageable);

        List<Comment> comments = commentsPage.getContent();

        return enrichCommentsWithExternalData(comments);
    }

    @Override
    public List<CommentDto> getCommentsAdmin(List<Long> events, List<Long> users, Pageable pageable) {
        log.info("Получение комментариев админом events: {}, users: {}", events, users);

        Page<Comment> commentsPage = commentRepository
                .findByEventIdInAndUserIdInOrderByCreatedAtDesc(events, users, pageable);

        List<Comment> comments = commentsPage.getContent();

        return enrichCommentsWithExternalData(comments);
    }

    @Override
    public Boolean commentExists(Long commentId) {
        return commentRepository.existsById(commentId);
    }

    @Override
    public Long getCountPublishedCommentsByEventId(Long eventId) {
        List<CommentStatus> publishedStatuses = List.of(CommentStatus.PUBLISHED, CommentStatus.EDITED);
        return commentRepository.countByEventIdAndStatusIn(eventId, publishedStatuses);
    }

    private List<CommentDto> enrichCommentsWithExternalData(List<Comment> comments) {
        if (comments.isEmpty()) {
            return List.of();
        }

        List<Long> eventIds = comments.stream()
                .map(Comment::getEventId)
                .distinct()
                .collect(Collectors.toList());

        List<Long> userIds = comments.stream()
                .map(Comment::getUserId)
                .distinct()
                .collect(Collectors.toList());

        Map<Long, EventShortDtoOut> eventsMap = eventClient.getEventsByIds(eventIds).stream()
                .collect(Collectors.toMap(EventShortDtoOut::getId, Function.identity()));

        Map<Long, UserDtoOut> usersMap = userClient.getUsersByIds(userIds).stream()
                .collect(Collectors.toMap(UserDtoOut::getId, Function.identity()));

        return comments.stream()
                .map(comment -> {
                    EventShortDtoOut event = eventsMap.get(comment.getEventId());
                    UserDtoOut author = usersMap.get(comment.getUserId());

                    if (event == null || author == null) {
                        log.warn("Не удалось получить данные для комментария {}: event={}, author={}",
                                comment.getId(), comment.getEventId(), comment.getUserId());
                        return null;
                    }

                    return commentMapper.toDto(comment, event, author);
                })
                .filter(commentDto -> commentDto != null)
                .collect(Collectors.toList());
    }
}