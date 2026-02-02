package ru.practicum.service;

public interface CollectorHandler<T> {
    void handle(T proto);
}
