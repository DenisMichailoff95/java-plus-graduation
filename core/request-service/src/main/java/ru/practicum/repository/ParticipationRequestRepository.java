package ru.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.model.ParticipationRequest;
import ru.practicum.enums.RequestStatus;

import java.util.List;

public interface ParticipationRequestRepository extends JpaRepository<ParticipationRequest, Long> {

    boolean existsByRequesterIdAndEventId(Long userId, Long eventId);

    List<ParticipationRequest> findAllByRequesterId(Long userId);

    List<ParticipationRequest> findAllByEventId(Long eventId);

    Integer countByEventIdAndStatus(Long eventId, RequestStatus status);

    @Query("""
            SELECT COUNT(pr)
            FROM ParticipationRequest pr
            WHERE pr.eventId = :eventId AND pr.status = 'CONFIRMED'""")
    int countConfirmedRequestsForEvent(@Param("eventId") Long eventId);

    @Query("""
            SELECT pr.eventId as eventId, COUNT(pr) as count
            FROM ParticipationRequest pr
            WHERE pr.eventId IN :eventIds
            AND pr.status = 'CONFIRMED'
            GROUP BY pr.eventId""")
    List<Object[]> findConfirmedRequestCountsByEventIds(@Param("eventIds") List<Long> eventIds);
}