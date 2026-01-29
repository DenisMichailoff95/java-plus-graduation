package ru.practicum.category.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.category.mapper.CategoryMapper;
import ru.practicum.category.model.Category;
import ru.practicum.category.repository.CategoryRepository;
import ru.practicum.dto.category.CategoryDto;
import ru.practicum.dto.category.CategoryDtoOut;
import ru.practicum.event.repository.EventRepository;
import ru.practicum.exception.NotFoundException;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CategoryServiceImpl implements CategoryService {

    private final CategoryRepository categoryRepository;
    private final EventRepository eventRepository;

    @Override
    public List<CategoryDtoOut> getAll(Integer offset, Integer limit) {
        Pageable pageable = PageRequest.of(offset / limit, limit);
        Page<Category> categoriesPage = categoryRepository.findAllByOrderById(pageable);
        return categoriesPage.getContent().stream()
                .map(CategoryMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    public CategoryDtoOut get(Long id) {
        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Category", id));
        return CategoryMapper.toDto(category);
    }

    @Override
    @Transactional
    public CategoryDtoOut add(CategoryDto categoryDto) {
        if (categoryRepository.existsByName(categoryDto.getName())) {
            throw new IllegalStateException("Category " + categoryDto.getName() + " already exists");
        }
        Category category = CategoryMapper.fromDto(categoryDto);
        Category saved = categoryRepository.save(category);
        return CategoryMapper.toDto(saved);
    }

    @Override
    @Transactional
    public CategoryDtoOut update(Long id, CategoryDto categoryDto) {
        Category existingByName = categoryRepository.findByName(categoryDto.getName());
        if (existingByName != null && !existingByName.getId().equals(id)) {
            throw new IllegalStateException("Category " + categoryDto.getName() + " already exists");
        }

        Category category = categoryRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Category", id));

        category.setName(categoryDto.getName());
        Category saved = categoryRepository.save(category);
        return CategoryMapper.toDto(saved);
    }

    @Override
    @Transactional
    public void delete(Long id) {
        if (!categoryRepository.existsById(id)) {
            throw new NotFoundException("Category", id);
        }

        if (eventRepository.existsByCategoryId(id)) {
            throw new IllegalStateException("Cannot delete category. There are events associated with it.");
        }

        categoryRepository.deleteById(id);
    }

    @Override
    public CategoryDtoOut getCategoryById(Long categoryId) {
        return get(categoryId);
    }

    @Override
    public List<CategoryDtoOut> getCategoriesByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }

        List<Category> categories = categoryRepository.findAllById(ids);

        return categories.stream()
                .map(CategoryMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    public Boolean categoryExists(Long categoryId) {
        return categoryRepository.existsById(categoryId);
    }
}