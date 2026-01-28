package ru.practicum.compilation.mapper;

import lombok.experimental.UtilityClass;
import ru.practicum.compilation.model.Compilation;
import ru.practicum.dto.compilation.CompilationDto;
import ru.practicum.dto.compilation.NewCompilationDto;

import java.util.Set;

@UtilityClass
public class CompilationMapper {

    public static Compilation toEntity(NewCompilationDto dto) {
        return Compilation.builder()
                .title(dto.getTitle())
                .pinned(dto.getPinned() != null && dto.getPinned())
                .eventIds(dto.getEvents() != null ? dto.getEvents() : Set.of())
                .build();
    }

    public static CompilationDto toDto(Compilation compilation) {
        return CompilationDto.builder()
                .id(compilation.getId())
                .title(compilation.getTitle())
                .pinned(compilation.getPinned())
                .events(null)
                .build();
    }
}