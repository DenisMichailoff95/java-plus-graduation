package ru.practicum.event.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.event.model.Event;

import java.util.List;
import java.util.Optional;

public interface EventRepository extends JpaRepository<Event, Long>, JpaSpecificationExecutor<Event> {

    // Найти события по инициатору
    @Query("SELECT e FROM Event e WHERE e.initiatorId = :userId")
    Page<Event> findByInitiatorId(@Param("userId") Long userId, Pageable pageable);

    // Найти опубликованное событие по ID
    @Query("SELECT e FROM Event e WHERE e.id = :id AND e.state = 'PUBLISHED'")
    Optional<Event> findPublishedById(@Param("id") Long id);

    // Проверить существование событий в категории
    boolean existsByCategoryId(Long categoryId);

    // Найти события по ID (для подборок)
    List<Event> findByIdIn(List<Long> eventIds);

    // Проверить существование события
    boolean existsById(Long id);
}