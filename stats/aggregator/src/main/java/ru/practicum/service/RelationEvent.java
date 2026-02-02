package ru.practicum.service;

public record RelationEvent(Long first, Long second) {

    public static RelationEvent create(Long a, Long b) {
        return a < b ? new RelationEvent(a, b) : new RelationEvent(b, a);
    }
}
