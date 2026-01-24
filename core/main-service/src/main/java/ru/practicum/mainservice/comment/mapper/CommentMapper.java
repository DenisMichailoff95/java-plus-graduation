package ru.practicum.mainservice.comment.mapper;

import lombok.experimental.UtilityClass;
import ru.practicum.mainservice.comment.dto.CommentDto;
import ru.practicum.mainservice.comment.model.Comment;
import ru.practicum.mainservice.event.mapper.EventMapper;
import ru.practicum.mainservice.user.mapper.UserMapper;

@UtilityClass
public class CommentMapper {
    public static CommentDto toDto(Comment comment) {
        return CommentDto.builder()
                .id(comment.getId())
                .text(comment.getText())
                .event(EventMapper.toShortDto(comment.getEvent()))
                .author(UserMapper.toDto(comment.getUser()))
                .createdAt(comment.getCreatedAt())
                .updatedAt(comment.getUpdatedAt())
                .status(comment.getStatus().name())
                .build();
    }
}
