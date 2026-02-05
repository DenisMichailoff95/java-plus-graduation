package ru.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import ru.practicum.converter.WeightConverter;
import ru.practicum.ewm.stats.avro.UserActionAvro;
import ru.practicum.mapper.UserActionMapper;
import ru.practicum.model.UserAction;
import ru.practicum.model.UserActionId;
import ru.practicum.repository.UserActionRepository;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserActionService {

    private final UserActionRepository repository;

    public void save(UserActionAvro avro) {

        log.info("сохранение действия {} пользователя: {} для события с id: {}",
                avro.getActionType(), avro.getUserId(), avro.getEventId());

        UserActionId id = UserActionMapper.toUserActionId(avro);
        Optional<UserAction> existingAction = repository.findById(id);

        if (shouldSaveAction(existingAction, avro)) {
            UserAction userAction = UserActionMapper.toUserAction(avro);
            repository.save(userAction);
        }
    }

    private boolean shouldSaveAction(Optional<UserAction> existingAction, UserActionAvro newAction) {

        if (existingAction.isEmpty()) {
            return true;
        }

        double existingScore = existingAction.get().getScore();
        double requiredWeight = WeightConverter.getWeightOnAction(newAction.getActionType());

        return existingScore < requiredWeight;
    }
}
