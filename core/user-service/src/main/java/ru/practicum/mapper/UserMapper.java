package ru.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;
import ru.practicum.dto.user.NewUserRequest;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.model.User;

@Mapper(componentModel = "spring")
public interface UserMapper {
    UserMapper INSTANCE = Mappers.getMapper(UserMapper.class);

    UserDtoOut toDto(User user);

    @Mapping(target = "id", ignore = true)
    User toEntity(NewUserRequest request);
}