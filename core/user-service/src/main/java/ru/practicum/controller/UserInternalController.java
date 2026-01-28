package ru.practicum.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.service.UserService;

import java.util.List;

@RestController
@RequestMapping("/internal/users")
@RequiredArgsConstructor
public class UserInternalController {

    private final UserService userService;

    @GetMapping("/{userId}")
    public UserDtoOut getUserById(@PathVariable Long userId) {
        return userService.getUserById(userId);
    }

    @GetMapping("/exists/{userId}")
    public Boolean checkUserExists(@PathVariable Long userId) {
        return userService.checkUserExists(userId);
    }

    @GetMapping("/exists/batch")
    public List<Boolean> checkUsersExist(@RequestParam List<Long> userIds) {
        return userIds.stream()
                .map(userService::checkUserExists)
                .toList();
    }

    @GetMapping("/batch")
    public List<UserDtoOut> getUsersByIds(@RequestParam List<Long> ids) {
        return userService.getUsersByIds(ids);
    }
}