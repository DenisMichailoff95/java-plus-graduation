package ru.practicum.server.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.dto.ViewStatsDTO;
import ru.practicum.server.model.Hit;

import java.time.LocalDateTime;
import java.util.List;

public interface HitRepository extends JpaRepository<Hit, Long> {

    @Query(
            """
                    SELECT new ru.practicum.dto.ViewStatsDTO(h.app, h.uri, COUNT(h.ip))
                    FROM Hit h
                    WHERE h.timestamp BETWEEN :start AND :end
                    AND (:uris IS NULL OR h.uri IN :uris)
                    GROUP BY h.app, h.uri
                    ORDER BY COUNT(h.ip) DESC
                    """
    )
    List<ViewStatsDTO> getStats(LocalDateTime start, LocalDateTime end, List<String> uris);

    @Query(
            """
                    SELECT new ru.practicum.dto.ViewStatsDTO(h.app, h.uri, COUNT(DISTINCT h.ip))
                    FROM Hit h
                    WHERE h.timestamp BETWEEN :start AND :end
                    AND (:uris IS NULL OR h.uri IN :uris)
                    GROUP BY h.app, h.uri
                    ORDER BY COUNT(DISTINCT h.ip) DESC
                    """
    )
    List<ViewStatsDTO> getUniqueStats(LocalDateTime start, LocalDateTime end, List<String> uris);

    // Добавить недостающие методы

    @Query("SELECT COUNT(h) FROM Hit h WHERE h.timestamp BETWEEN :start AND :end")
    Long countByTimestampBetween(@Param("start") LocalDateTime start, @Param("end") LocalDateTime end);

    @Query("SELECT COUNT(DISTINCT h.ip) FROM Hit h WHERE h.timestamp BETWEEN :start AND :end")
    Long countDistinctIpByTimestampBetween(@Param("start") LocalDateTime start, @Param("end") LocalDateTime end);

    @Modifying
    @Transactional
    @Query("DELETE FROM Hit h WHERE h.timestamp < :cutoffDate")
    Long deleteByTimestampBefore(@Param("cutoffDate") LocalDateTime cutoffDate);
}