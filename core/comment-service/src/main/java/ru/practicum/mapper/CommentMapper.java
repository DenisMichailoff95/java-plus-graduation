package ru.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.practicum.dto.comment.CommentDto;
import ru.practicum.dto.event.EventShortDtoOut;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.model.Comment;

@Mapper(componentModel = "spring")
public interface CommentMapper {

    @Mapping(target = "id", source = "comment.id")
    @Mapping(target = "text", source = "comment.text")
    @Mapping(target = "createdAt", source = "comment.createdAt")
    @Mapping(target = "updatedAt", source = "comment.updatedAt")
    @Mapping(target = "event", source = "event")
    @Mapping(target = "author", source = "author")
    @Mapping(target = "status", expression = "java(comment.getStatus() != null ? comment.getStatus().name() : null)")
    CommentDto toDto(Comment comment, EventShortDtoOut event, UserDtoOut author);
}