package ru.practicum.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.ewm.stats.proto.RecommendedEventProto;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RecommendationsMapper {

    public static RecommendedEventProto toRecommendedEventProto(Long eventId, Double score) {
        return RecommendedEventProto.newBuilder()
                .setEventId(eventId)
                .setScore(score)
                .build();
    }
}
