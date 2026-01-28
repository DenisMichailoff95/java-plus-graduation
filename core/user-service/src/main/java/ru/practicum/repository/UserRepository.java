package ru.practicum.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.model.User;

import java.util.List;

public interface UserRepository extends JpaRepository<User, Long> {

    @Query("SELECT u FROM User u " +
            "WHERE (:ids IS NULL OR u.id IN :ids) " +
            "ORDER BY u.id")
    List<User> findUsers(@Param("ids") List<Long> ids, Pageable pageable);

    List<User> findByIdIn(List<Long> ids);

    boolean existsById(Long id);
}