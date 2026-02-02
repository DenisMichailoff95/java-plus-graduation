package ru.practicum.category.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.practicum.category.model.Category;
import ru.practicum.dto.category.CategoryDto;
import ru.practicum.dto.category.CategoryDtoOut;

@Mapper(componentModel = "spring")
public interface CategoryMapper {

    CategoryDtoOut toDto(Category category);

    @Mapping(target = "id", ignore = true)
    Category fromDto(CategoryDto dto);
}