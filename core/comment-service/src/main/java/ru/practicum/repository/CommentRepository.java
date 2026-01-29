package ru.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import ru.practicum.model.Comment;
import ru.practicum.enums.CommentStatus;

import java.util.List;

public interface CommentRepository extends JpaRepository<Comment, Long> {

    Page<Comment> findByEventIdAndStatusInOrderByCreatedAtDesc(Long eventId, List<CommentStatus> statuses, Pageable pageable);

    Page<Comment> findByUserIdAndStatusNotOrderByCreatedAtDesc(Long userId, CommentStatus status, Pageable pageable);

    @Query("""
            SELECT c FROM Comment c WHERE
                (:eventIds IS NULL OR c.eventId IN :eventIds) AND
                (:userIds IS NULL OR c.userId IN :userIds)
            ORDER BY c.createdAt DESC
            """)
    Page<Comment> findByEventIdInAndUserIdInOrderByCreatedAtDesc(
            List<Long> eventIds,
            List<Long> userIds,
            Pageable pageable);

    Long countByEventIdAndStatusIn(Long eventId, List<CommentStatus> statuses);
}