package ru.practicum.mainservice.category.service;

import ru.practicum.mainservice.category.dto.CategoryDto;
import ru.practicum.mainservice.category.dto.CategoryDtoOut;

import java.util.Collection;

public interface CategoryService {

    Collection<CategoryDtoOut> getAll(Integer offset, Integer limit);

    CategoryDtoOut get(Long id);

    CategoryDtoOut add(CategoryDto categoryDto);

    CategoryDtoOut update(Long id, CategoryDto categoryDto);

    void delete(Long id);
}
