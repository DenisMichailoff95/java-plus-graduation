package ru.practicum.compilation.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.client.EventClient;
import ru.practicum.compilation.mapper.CompilationMapper;
import ru.practicum.compilation.model.Compilation;
import ru.practicum.compilation.repository.CompilationRepository;
import ru.practicum.dto.compilation.CompilationDto;
import ru.practicum.dto.compilation.NewCompilationDto;
import ru.practicum.dto.compilation.UpdateCompilationRequest;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.exception.ConditionNotMetException;
import ru.practicum.exception.NotFoundException;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class CompilationServiceImpl implements CompilationService {

    private final CompilationRepository compilationRepository;
    private final EventClient eventClient;

    @Override
    public List<CompilationDto> getCompilations(Boolean pinned, int from, int size) {
        Pageable pageable = PageRequest.of(from / size, size);
        Page<Compilation> compilationsPage = getCompilationsPage(pinned, pageable);
        List<Compilation> compilations = compilationsPage.getContent();

        return compilations.stream()
                .map(this::enrichCompilationWithEvents)
                .toList();
    }

    @Override
    public CompilationDto getCompilationById(Long compId) {
        Compilation compilation = compilationRepository.findById(compId)
                .orElseThrow(() -> new NotFoundException("Compilation", compId));
        return enrichCompilationWithEvents(compilation);
    }

    @Override
    @Transactional
    public CompilationDto createCompilation(NewCompilationDto newCompilationDto) {
        validateCompilationTitleUniqueness(newCompilationDto.getTitle());

        Compilation compilation = CompilationMapper.toEntity(newCompilationDto);
        Compilation saved = compilationRepository.save(compilation);
        return enrichCompilationWithEvents(saved);
    }

    @Override
    @Transactional
    public void deleteCompilation(Long compId) {
        Compilation compilation = compilationRepository.findById(compId)
                .orElseThrow(() -> new NotFoundException("Compilation", compId));
        compilationRepository.delete(compilation);
    }

    @Override
    @Transactional
    public CompilationDto updateCompilation(Long compId, UpdateCompilationRequest dto) {
        Compilation compilation = compilationRepository.findById(compId)
                .orElseThrow(() -> new NotFoundException("Compilation", compId));

        updateCompilationFields(compilation, dto);
        Compilation updated = compilationRepository.save(compilation);
        return enrichCompilationWithEvents(updated);
    }

    private Page<Compilation> getCompilationsPage(Boolean pinned, Pageable pageable) {
        return pinned != null
                ? compilationRepository.findByPinned(pinned, pageable)
                : compilationRepository.findAll(pageable);
    }

    private void validateCompilationTitleUniqueness(String title) {
        if (compilationRepository.existsByTitle(title)) {
            throw new ConditionNotMetException("A compilation with this title already exists");
        }
    }

    private void updateCompilationFields(Compilation compilation, UpdateCompilationRequest dto) {
        if (dto.getTitle() != null) {
            compilation.setTitle(dto.getTitle());
        }

        if (dto.getPinned() != null) {
            compilation.setPinned(dto.getPinned());
        }

        if (dto.getEvents() != null) {
            compilation.setEventIds(new HashSet<>(dto.getEvents()));
        }
    }

    private CompilationDto enrichCompilationWithEvents(Compilation compilation) {
        CompilationDto dto = CompilationMapper.toDto(compilation);
        dto.setEvents(getEventsForCompilation(compilation));
        return dto;
    }

    private List<EventShortDtoOut> getEventsForCompilation(Compilation compilation) {
        if (compilation.getEventIds() == null || compilation.getEventIds().isEmpty()) {
            return List.of();
        }

        try {
            return eventClient.getEventsByIds(new ArrayList<>(compilation.getEventIds()));
        } catch (Exception e) {
            log.warn("Event service unavailable for compilation {}, returning empty list: {}",
                    compilation.getId(), e.getMessage());
            return List.of();
        }
    }
}