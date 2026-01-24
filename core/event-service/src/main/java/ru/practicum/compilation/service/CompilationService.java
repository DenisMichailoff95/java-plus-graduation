package ru.practicum.compilation.service;

import ru.practicum.dto.compilation.CompilationDto;
import ru.practicum.dto.compilation.NewCompilationDto;
import ru.practicum.dto.compilation.UpdateCompilationRequest;

import java.util.List;

public interface CompilationService {
    CompilationDto getCompilationById(Long compId);

    List<CompilationDto> getCompilations(Boolean pinned, int from, int size);

    void deleteCompilation(Long compId);

    CompilationDto createCompilation(NewCompilationDto newCompilationDto);

    CompilationDto updateCompilation(Long compId, UpdateCompilationRequest dto);
}