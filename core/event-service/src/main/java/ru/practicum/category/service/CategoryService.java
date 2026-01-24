package ru.practicum.category.service;

import ru.practicum.dto.category.CategoryDto;
import ru.practicum.dto.category.CategoryDtoOut;

import java.util.List;

public interface CategoryService {
    List<CategoryDtoOut> getAll(Integer offset, Integer limit);

    CategoryDtoOut get(Long id);

    CategoryDtoOut add(CategoryDto categoryDto);

    CategoryDtoOut update(Long id, CategoryDto categoryDto);

    void delete(Long id);

    CategoryDtoOut getCategoryById(Long categoryId);

    List<CategoryDtoOut> getCategoriesByIds(List<Long> ids);

    Boolean categoryExists(Long categoryId);
}