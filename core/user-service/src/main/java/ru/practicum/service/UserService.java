package ru.practicum.service;

import ru.practicum.dto.user.NewUserRequest;
import ru.practicum.dto.user.UserDtoOut;

import java.util.List;

public interface UserService {

    UserDtoOut createUser(NewUserRequest request);

    List<UserDtoOut> getUsers(List<Long> ids, int from, int size);

    void deleteUser(Long userId);

    UserDtoOut getUserById(Long userId);

    Boolean checkUserExists(Long userId);

    List<Boolean> checkUsersExist(List<Long> userIds); // ← Новый метод

    List<UserDtoOut> getUsersByIds(List<Long> ids);
}