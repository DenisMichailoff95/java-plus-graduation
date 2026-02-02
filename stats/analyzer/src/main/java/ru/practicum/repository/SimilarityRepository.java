package ru.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.model.EventSimilarity;
import ru.practicum.model.EventSimilarityId;

import java.util.List;

public interface SimilarityRepository extends JpaRepository<EventSimilarity, EventSimilarityId> {

    @Query("""
    SELECT es, CASE WHEN es.first = :eventId THEN es.second ELSE es.first END AS similarEvent
    FROM EventSimilarity es
    WHERE (es.first = :eventId OR es.second = :eventId)
    AND NOT EXISTS (
            SELECT 1 FROM UserAction ua
            WHERE ua.userId = :userId
            AND ua.eventId = CASE WHEN es.first = :eventId THEN es.second ELSE es.first END
    )
    ORDER BY es.score DESC
    LIMIT :limit
    """)
    List<EventSimilarity> findByEventIdForUser(
            @Param("eventId") Long eventId,
            @Param("userId") Long userId,
            @Param("limit") long limit
    );

    @Query("""
    SELECT es
    FROM EventSimilarity es
    WHERE (es.first IN :recentEventIds AND es.second NOT IN :interactedEventIds)
    OR (es.second IN :recentEventIds AND es.first NOT IN :interactedEventIds)
    ORDER BY es.score DESC
    LIMIT :limit
    """)
    List<EventSimilarity> findSimilarUnseenEvents(
            @Param("recentEventIds") List<Long> recentEventIds,
            @Param("interactedEventIds") List<Long> interactedEventIds,
            @Param("limit") long limit
    );

    @Query("""
    SELECT es
    FROM EventSimilarity es
    WHERE
        (es.first IN :targetEventIds AND es.second IN :userEventIds)
        OR (es.second IN :targetEventIds AND es.first IN :userEventIds)
    ORDER BY es.score DESC
    LIMIT :limit
    """)
    List<EventSimilarity> findTopKSimilarUserEvents(
            @Param("targetEventIds") List<Long> targetEventIds,
            @Param("userEventIds") List<Long> userEventIds,
            @Param("limit") long limit
    );
}
