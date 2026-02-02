package ru.practicum.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.converter.WeightConverter;
import ru.practicum.ewm.stats.avro.UserActionAvro;
import ru.practicum.model.UserAction;
import ru.practicum.model.UserActionId;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserActionMapper {

    public static UserAction toUserAction(UserActionAvro avro) {
        return new UserAction(avro.getUserId(), avro.getEventId(),
                WeightConverter.getWeightOnAction(avro.getActionType()), avro.getTimestamp());
    }

    public static UserActionId toUserActionId(UserActionAvro avro) {
        return new UserActionId(avro.getUserId(), avro.getEventId());
    }
}
