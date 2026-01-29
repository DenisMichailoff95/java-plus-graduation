package ru.practicum.dto.participation;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventRequestStatusUpdateRequest {

    @NotEmpty(message = "Список ID заявок не должен быть пустым")
    private List<Long> requestIds;

    @NotNull(message = "Поле 'status' обязательно для заполнения")
    @Pattern(
            regexp = "CONFIRMED|REJECTED",
            message = "Допустимые значения для поля 'status': CONFIRMED или REJECTED"
    )
    private String status;
}
