package ru.practicum.mainservice.category.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import ru.practicum.mainservice.category.model.Category;

public interface CategoryRepository extends JpaRepository<Category, Long> {

    Page<Category> findAllByOrderById(Pageable pageable);

    boolean existsByName(String name);

    Category findByName(String name);
}