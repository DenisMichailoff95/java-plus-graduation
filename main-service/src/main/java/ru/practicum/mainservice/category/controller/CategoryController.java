package ru.practicum.mainservice.category.controller;

import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.mainservice.category.dto.CategoryDto;
import ru.practicum.mainservice.category.dto.CategoryDtoOut;
import ru.practicum.mainservice.category.service.CategoryService;

import java.util.Collection;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
public class CategoryController {

    private final CategoryService categoryService;

    @GetMapping("/categories")
    public Collection<CategoryDtoOut> getCategories(
            @RequestParam(name = "from", defaultValue = "0") @Min(0) Integer offset,
            @RequestParam(name = "size", defaultValue = "10") @Min(1) Integer limit
    ) {
        return categoryService.getAll(offset, limit);
    }

    @GetMapping("/categories/{id}")
    public CategoryDtoOut getCategory(@PathVariable @Min(1) Long id) {
        return categoryService.get(id);
    }

    @PostMapping("/admin/categories")
    @ResponseStatus(HttpStatus.CREATED)
    public CategoryDtoOut createCategory(@Validated @RequestBody CategoryDto categoryDto) {
        log.info("Create category '{}' by admin", categoryDto.getName());
        return categoryService.add(categoryDto);
    }

    @PatchMapping("/admin/categories/{id}")
    @ResponseStatus(HttpStatus.OK)
    public CategoryDtoOut updateCategory(@Validated @RequestBody CategoryDto categoryDto,
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
