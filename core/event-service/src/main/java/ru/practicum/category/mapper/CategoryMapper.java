package ru.practicum.category.mapper;

import lombok.experimental.UtilityClass;
import ru.practicum.category.model.Category;
import ru.practicum.dto.category.CategoryDto;
import ru.practicum.dto.category.CategoryDtoOut;

@UtilityClass
public class CategoryMapper {

    public static CategoryDtoOut toDto(Category category) {
        return new CategoryDtoOut(category.getId(), category.getName());
    }

    public static Category fromDto(CategoryDto dto) {
        return new Category(null, dto.getName());
    }
}