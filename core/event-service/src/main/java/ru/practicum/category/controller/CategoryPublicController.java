package ru.practicum.category.controller;

import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.category.service.CategoryService;
import ru.practicum.dto.category.CategoryDtoOut;

import java.util.List;

@Slf4j
@Validated
@RestController
@RequiredArgsConstructor
public class CategoryPublicController {

    private final CategoryService categoryService;

    @GetMapping("/categories")
    public List<CategoryDtoOut> getCategories(
            @RequestParam(name = "from", defaultValue = "0") @Min(0) Integer offset,
            @RequestParam(name = "size", defaultValue = "10") @Min(1) Integer limit) {
        return categoryService.getAll(offset, limit);
    }

    @GetMapping("/categories/{id}")
    public CategoryDtoOut getCategory(@PathVariable @Min(1) Long id) {
        return categoryService.get(id);
    }
}