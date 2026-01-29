package ru.practicum.dto.comment;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.user.UserDtoOut;

import java.time.LocalDateTime;

import static ru.practicum.constants.Constants.DATE_TIME_FORMAT;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommentDto {
    private Long id;
    private String text;
    private EventShortDtoOut event;
    private UserDtoOut author;

    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime createdAt;

    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime updatedAt;

    private String status;
}
