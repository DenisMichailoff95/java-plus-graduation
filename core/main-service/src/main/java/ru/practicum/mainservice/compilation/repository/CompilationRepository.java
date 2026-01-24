package ru.practicum.mainservice.compilation.repository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.mainservice.compilation.model.Compilation;

import java.util.List;

public interface CompilationRepository extends JpaRepository<Compilation, Long> {
    List<Compilation> findByPinned(Boolean pinned, Pageable pageable);

    boolean existsByTitle(String title);
}