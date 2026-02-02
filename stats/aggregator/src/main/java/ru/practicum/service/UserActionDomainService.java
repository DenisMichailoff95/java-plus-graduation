package ru.practicum.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.practicum.ewm.stats.avro.ActionTypeAvro;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;
import ru.practicum.exception.IncorrectActionTypeException;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
public class UserActionDomainService {

    private final Map<Long, Double> eventWeightSumMap;
    private final Map<RelationEvent, Double> eventsSimilarityMap;
    private final Map<Long, Double> sqrtCacheMap;
    private final Map<Long, Map<Long, Double>> usersFeedbackMap;
    private final Map<RelationEvent, Double> eventsMinWeightSumMap;

    public UserActionDomainService() {
        usersFeedbackMap = new HashMap<>();
        eventsMinWeightSumMap = new HashMap<>();
        eventWeightSumMap = new HashMap<>();
        eventsSimilarityMap = new HashMap<>();
        sqrtCacheMap = new HashMap<>();
    }

    public List<EventSimilarityAvro> calculateSimilarityEvents(UserActionAvro avro) throws IncorrectActionTypeException {

        Long userId = avro.getUserId();
        Long eventId = avro.getEventId();
        log.info("User action: {} with the event: {}", userId, eventId);

        Double newWeight = convertActionToWeight(avro.getActionType());
        log.info("User action weight: {}", newWeight);

        Map<Long, Double> userRatingsMap = usersFeedbackMap.computeIfAbsent(eventId, k -> new HashMap<>());
        Double oldWeight = userRatingsMap.getOrDefault(userId, 0.0);

        log.info("Comparison of old and new weight: {} and {}", oldWeight, newWeight);
        if (oldWeight < newWeight) {
            log.info("The new weight is greater than the old one");
            userRatingsMap.put(userId, newWeight);
            return determineSimilarity(eventId, userId, oldWeight, newWeight, avro.getTimestamp());
        } else {
            return Collections.emptyList();
        }
    }


    private List<EventSimilarityAvro> determineSimilarity(Long eventId, Long userId, Double oldWeight, Double newWeight, Instant timestamp) {

        double updatedEventSum = eventWeightSumMap.getOrDefault(eventId, 0.0) - oldWeight + newWeight;
        eventWeightSumMap.put(eventId, updatedEventSum);
        sqrtCacheMap.remove(eventId);

        List<EventSimilarityAvro> similarityMessages = new ArrayList<>();

        for (Map.Entry<Long, Map<Long, Double>> entry : usersFeedbackMap.entrySet()) {
            Long currentEventId = entry.getKey();
            Map<Long, Double> userFeedbackMap = entry.getValue();

            if (!userFeedbackMap.containsKey(userId) || Objects.equals(currentEventId, eventId)) continue;

            double userConvergenceWeight = userFeedbackMap.get(userId);
            RelationEvent eventPair = RelationEvent.create(eventId, currentEventId);

            double previousMinSum = eventsMinWeightSumMap.getOrDefault(eventPair, 0.0);
            double updatedMinSum = previousMinSum - Math.min(oldWeight, userConvergenceWeight) + Math.min(newWeight, userConvergenceWeight);

            eventsMinWeightSumMap.put(eventPair, updatedMinSum);
            double similarityScore = calculateSimilarity(eventPair, updatedMinSum);
            eventsSimilarityMap.put(eventPair, similarityScore);

            EventSimilarityAvro message = EventSimilarityAvro.newBuilder()
                    .setEventA(eventPair.first())
                    .setEventB(eventPair.second())
                    .setScore(similarityScore)
                    .setTimestamp(timestamp)
                    .build();
            similarityMessages.add(message);
        }

        return similarityMessages;
    }

    private Double convertActionToWeight(ActionTypeAvro action) throws IncorrectActionTypeException {
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
            default ->
                    throw new IncorrectActionTypeException("User action type: {0} - \n" +
                            "doesn't exist", action);
        }
    }

    private double calculateSimilarity(RelationEvent pair, double sumCommon) {
        return sumCommon / (getSumOfSqrt(pair.first()) * getSumOfSqrt(pair.second()));
    }

    private double getSumOfSqrt(Long eventId) {
        return sqrtCacheMap.computeIfAbsent(eventId, id -> Math.sqrt(eventWeightSumMap.getOrDefault(id, 0.0)));
    }
}
