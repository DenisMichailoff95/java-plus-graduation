package ru.practicum.category.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.client.CategoryClient;
import ru.practicum.category.service.CategoryService;
import ru.practicum.dto.category.CategoryDtoOut;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/categories")
public class CategoryClientController implements CategoryClient {

    private final CategoryService categoryService;

    @Override
    @GetMapping("/{categoryId}")
    public CategoryDtoOut getCategoryById(@PathVariable Long categoryId) {
        return categoryService.getCategoryById(categoryId);
    }

    @Override
    @GetMapping
    public List<CategoryDtoOut> getCategoriesByIds(@RequestParam List<Long> ids) {
        return categoryService.getCategoriesByIds(ids);
    }

    @Override
    @GetMapping("/exists/{categoryId}")
    public Boolean categoryExists(@PathVariable Long categoryId) {
        return categoryService.categoryExists(categoryId);
    }
}