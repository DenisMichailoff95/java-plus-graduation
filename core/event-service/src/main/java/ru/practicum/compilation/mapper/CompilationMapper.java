package ru.practicum.compilation.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.practicum.compilation.model.Compilation;
import ru.practicum.dto.compilation.CompilationDto;
import ru.practicum.dto.compilation.NewCompilationDto;

import java.util.HashSet;
import java.util.Set;

@Mapper(componentModel = "spring")
public interface CompilationMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "pinned", expression = "java(dto.getPinned() != null && dto.getPinned())")
    @Mapping(target = "eventIds", expression = "java(mapEvents(dto.getEvents()))")
    Compilation toEntity(NewCompilationDto dto);

    @Mapping(target = "events", ignore = true)
    CompilationDto toDto(Compilation compilation);

    default Set<Long> mapEvents(Set<Long> events) {
        return events != null ? events : new HashSet<>();
    }
}