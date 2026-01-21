package ru.practicum.mainservice.exception;

import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
@RestControllerAdvice
public class ErrorHandler {

    private static final String UNEXPECTED_ERROR_MESSAGE = "An unexpected error occurred";
    private static final String VALIDATION_ERROR_MESSAGE = "Validation failed";
    private static final String CONSTRAINT_VIOLATION_MESSAGE = "Constraint violation";

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleMethodArgumentNotValidException(MethodArgumentNotValidException ex) {
        String errorMessage = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(FieldError::getDefaultMessage)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(VALIDATION_ERROR_MESSAGE);

        log.warn("Validation error: {}", errorMessage);

        return buildErrorResponse(
                errorMessage,
                VALIDATION_ERROR_MESSAGE,
                HttpStatus.BAD_REQUEST
        );
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFoundException(NotFoundException ex) {
        log.warn("Resource not found: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                "The required object was not found.",
                HttpStatus.NOT_FOUND
        );
    }

    @ExceptionHandler({ConditionNotMetException.class, IllegalStateException.class})
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse handleConflictExceptions(RuntimeException ex) {
        log.warn("Conflict: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                "Condition not met.",
                HttpStatus.CONFLICT
        );
    }

    @ExceptionHandler(NoAccessException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ErrorResponse handleNoAccessException(NoAccessException ex) {
        log.warn("Access denied: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                "No access.",
                HttpStatus.FORBIDDEN
        );
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleMissingServletRequestParameterException(MissingServletRequestParameterException ex) {
        String message = String.format("Required parameter '%s' is missing", ex.getParameterName());
        log.warn("Missing parameter: {}", message);

        return buildErrorResponse(
                message,
                "Missing parameter.",
                HttpStatus.BAD_REQUEST
        );
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleMethodArgumentTypeMismatchException(MethodArgumentTypeMismatchException ex) {
        String message = String.format("Parameter '%s' has invalid value '%s'",
                ex.getName(), ex.getValue());
        log.warn("Type mismatch: {}", message);

        return buildErrorResponse(
                message,
                "Invalid parameter type.",
                HttpStatus.BAD_REQUEST
        );
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleConstraintViolationException(ConstraintViolationException ex) {
        log.warn("Constraint violation: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                CONSTRAINT_VIOLATION_MESSAGE,
                HttpStatus.BAD_REQUEST
        );
    }

    @ExceptionHandler(InvalidRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleInvalidRequestException(InvalidRequestException ex) {
        log.warn("Invalid request: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                "Bad request.",
                HttpStatus.BAD_REQUEST
        );
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse onDataIntegrityViolationException(final DataIntegrityViolationException e) {
        String rootCause = Objects.requireNonNull(e.getRootCause()).getMessage();
        log.error("Data integrity violation: {}", rootCause);

        return buildErrorResponse(
                e.getMessage(),
                rootCause,
                HttpStatus.CONFLICT
        );
    }

    @ExceptionHandler(HttpClientErrorException.class)
    public ResponseEntity<ErrorResponse> handleHttpClientErrorException(HttpClientErrorException e) {
        log.error("HTTP client error: {} {}", e.getStatusCode(), e.getMessage());

        ErrorResponse errorResponse = buildErrorResponse(
                e.getMessage(),
                "External service error",
                HttpStatus.valueOf(e.getStatusCode().value())
        );

        return new ResponseEntity<>(errorResponse, e.getStatusCode());
    }

    @ExceptionHandler(ResourceAccessException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public ErrorResponse handleResourceAccessException(ResourceAccessException e) {
        log.error("Service unavailable: {}", e.getMessage());

        return buildErrorResponse(
                "Service temporarily unavailable",
                "External service connection failed",
                HttpStatus.SERVICE_UNAVAILABLE
        );
    }

    @ExceptionHandler(ForbiddenException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ErrorResponse handleForbiddenException(ForbiddenException ex) {
        log.warn("Forbidden: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                "Access denied.",
                HttpStatus.FORBIDDEN
        );
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleIllegalArgumentException(IllegalArgumentException ex) {
        log.warn("Illegal argument: {}", ex.getMessage());

        return buildErrorResponse(
                ex.getMessage(),
                "Invalid argument.",
                HttpStatus.BAD_REQUEST
        );
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleException(final Exception e) {
        log.error("Unexpected error: {}", e.getMessage(), e);

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        String stackTrace = sw.toString();

        // В продакшене не выводим стектрейс
        boolean isProduction = "prod".equals(System.getenv("SPRING_PROFILES_ACTIVE"));
        String reason = isProduction ? UNEXPECTED_ERROR_MESSAGE : stackTrace;

        return buildErrorResponse(
                UNEXPECTED_ERROR_MESSAGE,
                reason,
                HttpStatus.INTERNAL_SERVER_ERROR
        );
    }

    private ErrorResponse buildErrorResponse(String message, String reason, HttpStatus status) {
        return ErrorResponse.builder()
                .message(message)
                .reason(reason)
                .status(status)
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public ErrorResponse handleRateLimitException(final RuntimeException e) {
        log.warn("Rate limit exceeded: {}", e.getMessage());

        return buildErrorResponse(
                "Too many requests",
                "Rate limit exceeded. Please try again later.",
                HttpStatus.TOO_MANY_REQUESTS
        );
    }
}