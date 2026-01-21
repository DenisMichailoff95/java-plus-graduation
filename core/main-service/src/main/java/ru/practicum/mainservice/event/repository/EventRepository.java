package ru.practicum.mainservice.event.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.mainservice.event.model.Event;

import java.util.Optional;

public interface EventRepository extends
        JpaRepository<Event, Long>,
        JpaSpecificationExecutor<Event> {

    @Query("SELECT e FROM Event e WHERE e.initiator.id = :userId")
    Page<Event> findByInitiatorId(@Param("userId") Long userId, Pageable pageable);

    @Query(value = """
            SELECT e FROM Event e
            WHERE e.id = :id AND e.state = 'PUBLISHED'
            """)
    Optional<Event> findPublishedById(@Param("id") Long id);

    boolean existsByCategoryId(Long categoryId);
}
