package ru.practicum.mainservice.comment.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.practicum.mainservice.event.dto.EventShortDtoOut;
import ru.practicum.mainservice.user.dto.UserDtoOut;

import java.time.LocalDateTime;

import static ru.practicum.mainservice.constants.Constants.DATE_TIME_FORMAT;

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
