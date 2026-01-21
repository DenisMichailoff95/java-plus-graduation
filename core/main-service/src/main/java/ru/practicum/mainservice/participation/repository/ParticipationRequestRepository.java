package ru.practicum.mainservice.participation.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.mainservice.participation.model.ParticipationRequest;
import ru.practicum.mainservice.participation.model.RequestStatus;

import java.util.List;

public interface ParticipationRequestRepository extends JpaRepository<ParticipationRequest, Long> {
    boolean existsByRequesterIdAndEventId(Long userId, Long eventId);

    List<ParticipationRequest> findAllByRequesterId(Long userId);

    List<ParticipationRequest> findAllByEventId(Long eventId);

    Integer countByEventIdAndStatus(Long eventId, RequestStatus status);

    @Query("""
            SELECT COUNT(pr)
            FROM ParticipationRequest pr
            WHERE pr.event.id = :eventId AND pr.status = 'CONFIRMED'""")
    int countConfirmedRequestsForEvent(@Param("eventId") Long eventId);

    @Query("""
            SELECT pr.event.id as eventId, COUNT(pr) as confirmedCount
            FROM ParticipationRequest pr
            WHERE pr.event.id IN :eventIds
            AND pr.status = 'CONFIRMED'
            GROUP BY pr.event.id""")
    List<Object[]> findConfirmedRequestCountsByEventIds(@Param("eventIds") List<Long> eventIds);

}
