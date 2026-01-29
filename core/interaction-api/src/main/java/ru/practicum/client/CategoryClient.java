package ru.practicum.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import ru.practicum.dto.category.CategoryDtoOut;

import java.util.List;

@FeignClient(name = "event-service", contextId = "categoryClient", path = "/internal/categories")
@CircuitBreaker(name = "category-service")
@Retry(name = "category-service")
public interface CategoryClient {

    @GetMapping("/{categoryId}")
    CategoryDtoOut getCategoryById(@PathVariable Long categoryId);

    @GetMapping
    List<CategoryDtoOut> getCategoriesByIds(@RequestParam List<Long> ids);

    @GetMapping("/exists/{categoryId}")
    Boolean categoryExists(@PathVariable Long categoryId);
}