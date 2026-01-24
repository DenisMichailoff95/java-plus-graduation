package ru.practicum.dto.event;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

import static ru.practicum.constants.Constants.DATE_TIME_FORMAT;


@Getter
@Setter
@ToString
public class EventUpdateDto {

    @Size(min = 3, max = 120, message = "Длина заголовка должна составлять от 3 до 120 символов")
    private String title;

    @Size(min = 20, max = 2000, message = "Длина аннотации должна составлять от 20 до 2000 символов")
    private String annotation;

    @JsonProperty("category")
    private Long categoryId;

    @Size(min = 20, max = 7000, message = "Длина описания должна составлять от 20 до 7000 символов")
    private String description;

    @Future(message = "Дата проведения мероприятия должна быть назначена в будущем")
    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime eventDate;

    private LocationDto location;

    private Boolean paid;

    @Min(0)
    private Integer participantLimit;
    private Boolean requestModeration;

    private StateAction stateAction;

    public enum StateAction {
        SEND_TO_REVIEW,
        CANCEL_REVIEW
    }
}
