package ru.practicum.category.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.category.service.CategoryService;
import ru.practicum.dto.category.CategoryDto;
import ru.practicum.dto.category.CategoryDtoOut;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
public class CategoryAdminController {

    private final CategoryService categoryService;

    @PostMapping("/admin/categories")
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryDtoOut createCategory(@Valid @RequestBody CategoryDto categoryDto) {
        log.info("Create category '{}' by admin", categoryDto.getName());
        return categoryService.add(categoryDto);
    }

    @PatchMapping("/admin/categories/{id}")
    @ResponseStatus(HttpStatus.OK)
    public CategoryDtoOut updateCategory(@Valid @RequestBody CategoryDto categoryDto,
                                         @PathVariable Long id) {
        log.info("Update category id:{} by admin", id);
        return categoryService.update(id, categoryDto);
    }

    @DeleteMapping("/admin/categories/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteCategory(@PathVariable Long id) {
        log.info("delete category id:{} by admin", id);
        categoryService.delete(id);
    }
}