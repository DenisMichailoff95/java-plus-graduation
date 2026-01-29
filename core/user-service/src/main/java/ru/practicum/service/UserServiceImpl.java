package ru.practicum.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.dto.user.NewUserRequest;
import ru.practicum.dto.user.UserDtoOut;
import ru.practicum.exception.NotFoundException;
import ru.practicum.mapper.UserMapper;
import ru.practicum.model.User;
import ru.practicum.repository.UserRepository;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;

    @Override
    @Transactional
    public UserDtoOut createUser(NewUserRequest request) {
        User user = UserMapper.toEntity(request);
        return UserMapper.toDto(userRepository.save(user));
    }

    @Override
    public List<UserDtoOut> getUsers(List<Long> ids, int from, int size) {
        Pageable pageable = PageRequest.of(from / size, size);
        List<User> users;
        if (ids == null || ids.isEmpty()) {
            users = userRepository.findUsers(null, pageable);
        } else {
            users = userRepository.findUsers(ids, pageable);
        }
        return users.stream().map(UserMapper::toDto).collect(Collectors.toList());
    }

    @Override
    @Transactional
    public void deleteUser(Long userId) {
        if (!userRepository.existsById(userId)) {
            throw new NotFoundException("User", userId);
        }
        userRepository.deleteById(userId);
    }

    @Override
    public UserDtoOut getUserById(Long userId) {
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new NotFoundException("User", userId));
        return UserMapper.toDto(user);
    }

    @Override
    public Boolean checkUserExists(Long userId) {
        return userRepository.existsById(userId);
    }

    @Override
    public List<Boolean> checkUsersExist(List<Long> userIds) {
        return userIds.stream()
                .map(userRepository::existsById)
                .toList();
    }

    @Override
    public List<UserDtoOut> getUsersByIds(List<Long> ids) {
        List<User> users = userRepository.findByIdIn(ids);
        return users.stream()
                .map(UserMapper::toDto)
                .collect(Collectors.toList());
    }
}