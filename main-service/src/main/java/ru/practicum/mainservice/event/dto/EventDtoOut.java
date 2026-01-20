package ru.practicum.mainservice.event.dto;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.mainservice.category.dto.CategoryDtoOut;
import ru.practicum.mainservice.event.model.EventState;
import ru.practicum.mainservice.user.dto.UserDtoOut;

import java.time.LocalDateTime;

import static ru.practicum.mainservice.constants.Constants.DATE_TIME_FORMAT;

@Getter
@Setter
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class EventDtoOut {

    private Long id;
    private String title;
    private String annotation;
    private String description;
    private CategoryDtoOut category;
    private UserDtoOut initiator;
    private LocationDto location;

    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime eventDate;

    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime createdOn;

    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime publishedOn;

    private Boolean paid;
    private Integer participantLimit;
    private Boolean requestModeration;
    private EventState state;
    private Integer confirmedRequests;

    @Builder.Default
    private Long views = 0L;
}