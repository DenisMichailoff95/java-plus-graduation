package ru.practicum.exception;

public class NoAccessException extends RuntimeException {
    public NoAccessException(String s) {
        super(s);
    }
}
