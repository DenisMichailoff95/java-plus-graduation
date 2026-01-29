package ru.practicum.client;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import ru.practicum.dto.user.UserDtoOut;

import java.util.List;

@FeignClient(name = "user-service", contextId = "userClient", path = "/internal/users")
@CircuitBreaker(name = "user-service")
@Retry(name = "user-service")
public interface UserClient {

    @GetMapping("/{userId}")
    UserDtoOut getUserById(@PathVariable Long userId);

    @GetMapping("/batch")
    List<UserDtoOut> getUsersByIds(@RequestParam List<Long> ids);

    @GetMapping("/exists/{userId}")
    Boolean userExists(@PathVariable Long userId);

    @GetMapping("/exists/batch")
    List<Boolean> usersExist(@RequestParam List<Long> userIds);
}