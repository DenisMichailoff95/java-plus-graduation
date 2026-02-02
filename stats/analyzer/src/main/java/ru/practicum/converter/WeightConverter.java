package ru.practicum.converter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.ewm.stats.avro.ActionTypeAvro;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WeightConverter {

    public static double getWeightOnAction(ActionTypeAvro action) {
        switch (action) {
            case VIEW -> {
                return 0.4;
            }
            case REGISTER -> {
                return 0.8;
            }
            case LIKE -> {
                return 1.0;
            }
            default -> {
                return 0.0;
            }
        }
    }
}
