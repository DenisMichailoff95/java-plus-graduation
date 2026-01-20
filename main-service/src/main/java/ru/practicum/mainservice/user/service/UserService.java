package ru.practicum.mainservice.user.service;

import ru.practicum.mainservice.user.dto.NewUserRequest;
import ru.practicum.mainservice.user.dto.UserDtoOut;

import java.util.List;

public interface UserService {

    UserDtoOut createUser(NewUserRequest request);

    List<UserDtoOut> getUsers(List<Long> ids, int from, int size);

    void deleteUser(Long userId);
}