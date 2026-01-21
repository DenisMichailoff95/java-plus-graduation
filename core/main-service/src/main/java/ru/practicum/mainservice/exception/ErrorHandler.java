package ru.practicum.mainservice.exception;

import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.util.Objects;

@Slf4j
@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleMethodArgumentNotValidException(MethodArgumentNotValidException ex) {
        String errorMessage = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(FieldError::getDefaultMessage)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("Validation failed");

        return ErrorResponse.builder()
                .message(errorMessage)
                .status(HttpStatus.METHOD_NOT_ALLOWED)
                .reason("Method argument is not valid.")
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFoundException(NotFoundException ex) {
        return ErrorResponse.builder()
                .message(ex.getMessage())
                .status(HttpStatus.NOT_FOUND)
                .reason("The required object was not found.")
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler({ConditionNotMetException.class, IllegalStateException.class})
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse handleConflictExceptions(RuntimeException ex) {
        return ErrorResponse.builder()
                .message(ex.getMessage())
                .status(HttpStatus.CONFLICT)
                .reason("Condition not met.")
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(NoAccessException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ErrorResponse handleNoAccessException(NoAccessException ex) {
        return ErrorResponse.builder()
                .message(ex.getMessage())
                .status(HttpStatus.FORBIDDEN)
                .reason("No access.")
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleMissingServletRequestParameterException(MissingServletRequestParameterException ex) {
        return ErrorResponse.builder()
                .message("Required parameter '" + ex.getParameterName() + "' is missing")
                .reason("Missing parameter.")
                .status(HttpStatus.BAD_REQUEST)
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleConstraintViolationException(ConstraintViolationException ex) {
        return ErrorResponse.builder()
                .message(ex.getMessage())
                .status(HttpStatus.CONFLICT)
                .reason("Constraint violation")
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(InvalidRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleInvalidRequestException(InvalidRequestException ex) {
        return ErrorResponse.builder()
                .message(ex.getMessage())
                .reason("Bad request.")
                .status(HttpStatus.BAD_REQUEST)
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(DataIntegrityViolationException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public ErrorResponse onDataIntegrityViolationException(final DataIntegrityViolationException e) {
        log.error("409 {}", e.getMessage());
        return ErrorResponse.builder()
                .message(e.getMessage())
                .status(HttpStatus.CONFLICT)
                .reason(Objects.requireNonNull(e.getRootCause()).getMessage())
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleException(final Exception e) {
        log.warn("Error 500 {}", e.getMessage(), e);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        String stackTrace = sw.toString();
        return ErrorResponse.builder()
                .message(e.getMessage())
                .reason(stackTrace)
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .timestamp(LocalDateTime.now())
                .build();
    }

    @ExceptionHandler(ForbiddenException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ErrorResponse handleForbiddenException(ForbiddenException ex) {
        return ErrorResponse.builder()
                .message(ex.getMessage())
                .status(HttpStatus.FORBIDDEN)
                .reason("Access denied.")
                .timestamp(LocalDateTime.now())
                .build();
    }
}