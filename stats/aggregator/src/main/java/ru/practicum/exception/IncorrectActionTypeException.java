package ru.practicum.exception;

import java.text.MessageFormat;
import java.util.function.Supplier;

public class IncorrectActionTypeException extends RuntimeException {

    public IncorrectActionTypeException(String message) {
        super(message);
    }

    public IncorrectActionTypeException(String message, Object... args) {
        super(MessageFormat.format(message, args));
    }

    public static Supplier<IncorrectActionTypeException> incorrectActionTypeExceptionSupplier(String message, Object... args) {
        return () -> new IncorrectActionTypeException(message, args);
    }

    public static Supplier<IncorrectActionTypeException> incorrectActionTypeExceptionSupplier(String message) {
        return () -> new IncorrectActionTypeException(message);
    }
}
