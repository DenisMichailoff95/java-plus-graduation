package ru.practicum.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

import static ru.practicum.constants.Constants.DATE_TIME_FORMAT;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventCreateDto {

    @NotBlank
    @Size(min = 3, max = 120, message = "Длина заголовка должна составлять от 3 до 120 символов")
    private String title;

    @NotBlank
    @Size(min = 20, max = 2000, message = "Длина аннотации должна составлять от 20 до 2000 символов")
    private String annotation;

    @NotNull
    @JsonProperty("category")
    private Long categoryId;

    @NotBlank
    @Size(min = 20, max = 7000, message = "Длина описания должна составлять от 20 до 7000 символов")
    private String description;

    @NotNull(message = "Дата события не может быть пустой")
    @Future(message = "Дата проведения мероприятия должна быть назначена в будущем")
    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime eventDate;

    @NotNull
    private LocationDto location;

    private Boolean paid = false;

    @Min(0)
    private Integer participantLimit = 0;
    private Boolean requestModeration = true;
}