package ru.practicum.exception;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

import static ru.practicum.constants.Constants.DATE_TIME_FORMAT;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse {
    private String message;
    private String reason;
    private HttpStatus status;

    @JsonFormat(pattern = DATE_TIME_FORMAT)
    private LocalDateTime timestamp;
}
