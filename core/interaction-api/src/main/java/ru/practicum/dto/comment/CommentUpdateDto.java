package ru.practicum.dto.comment;

import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommentUpdateDto {

    @Size(min = 1, max = 2000, message = "Длина комментария должна быть от 1 до 2000 символов")
    private String text;
}
