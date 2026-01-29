package ru.practicum.mapper;

import lombok.experimental.UtilityClass;
import ru.practicum.dto.comment.CommentDto;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.model.Comment;

@UtilityClass
public class CommentMapper {
    public static CommentDto toDto(Comment comment, EventShortDtoOut event, UserDtoOut author) {
        return CommentDto.builder()
                .id(comment.getId())
                .text(comment.getText())
                .event(event)
                .author(author)
                .createdAt(comment.getCreatedAt())
                .updatedAt(comment.getUpdatedAt())
                .status(comment.getStatus().name())
                .build();
    }
}