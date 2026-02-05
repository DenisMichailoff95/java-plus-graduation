package ru.practicum.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.practicum.ewm.stats.proto.InteractionsCountRequestProto;
import ru.practicum.ewm.stats.proto.RecommendedEventProto;
import ru.practicum.ewm.stats.proto.SimilarEventsRequestProto;
import ru.practicum.ewm.stats.proto.UserPredictionsRequestProto;
import ru.practicum.mapper.RecommendationsMapper;
import ru.practicum.model.EventSimilarity;
import ru.practicum.model.UserAction;
import ru.practicum.repository.SimilarityRepository;
import ru.practicum.repository.UserActionRepository;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RecommendationsService {

    private final UserActionRepository actionRepository;
    private final SimilarityRepository similarityRepository;

    public void getRecommendationsForUser(UserPredictionsRequestProto request,
                                          StreamObserver<RecommendedEventProto> responseObserver) {

        long userId = request.getUserId();
        long maxResult = request.getMaxResult();

        List<Long> userRecentActionsEvents = actionRepository.findRecentEventIdsByUserId(userId, maxResult);
        if (userRecentActionsEvents.isEmpty()) {
            return;
        }

        List<UserAction> allUserInteractions = actionRepository.findAllInteractionsByUser(userId);
        List<Long> userEventIds = extractEventIds(allUserInteractions);

        List<Long> similarities = findSimilarUnseenEvents(userRecentActionsEvents, userEventIds, maxResult);
        Map<Long, Double> eventScore = createEventScoreMap(allUserInteractions);

        List<RecommendedEventProto> recommendations = generateRecommendations(
                similarities, userEventIds, eventScore, maxResult
        );

        sendRecommendations(recommendations, responseObserver);
    }

    public void getSimilarEvents(SimilarEventsRequestProto request,
                                 StreamObserver<RecommendedEventProto> responseObserver) {

        List<RecommendedEventProto> eventsSimilarity = similarityRepository.findByEventIdForUser(request.getEventId(),
                        request.getUserId(), request.getMaxResult()).stream()
                .map(e -> RecommendationsMapper.toRecommendedEventProto(
                        e.getFirst() == request.getEventId() ? e.getSecond() : e.getFirst(), e.getScore()
                )).toList();

        for (RecommendedEventProto proto : eventsSimilarity) {
            responseObserver.onNext(proto);
        }
    }

    public void getInteractionsCount(InteractionsCountRequestProto request,
                                     StreamObserver<RecommendedEventProto> responseObserver) {

        List<RecommendedEventProto> eventInteractions = actionRepository.findInteractions(request.getEventIdList())
                .stream().map(
                        e -> RecommendedEventProto.newBuilder().setEventId((long) e[0]).setScore((double) e[1]).build()
                ).toList();

        for (RecommendedEventProto proto : eventInteractions) {
            responseObserver.onNext(proto);
        }
    }

    private List<Long> extractEventIds(List<UserAction> userActions) {
        return userActions.stream()
                .map(UserAction::getEventId)
                .toList();
    }

    private List<Long> findSimilarUnseenEvents(List<Long> recentEvents, List<Long> userEventIds, long maxResult) {
        return similarityRepository.findSimilarUnseenEvents(recentEvents, userEventIds, maxResult)
                .stream()
                .map(es -> recentEvents.contains(es.getFirst()) ? es.getSecond() : es.getFirst())
                .toList();
    }

    private Map<Long, Double> createEventScoreMap(List<UserAction> userActions) {
        return userActions.stream()
                .collect(Collectors.toMap(
                        UserAction::getEventId,
                        UserAction::getScore
                ));
    }

    private List<RecommendedEventProto> generateRecommendations(List<Long> candidateEvents,
                                                                List<Long> userEventIds,
                                                                Map<Long, Double> eventScore,
                                                                long maxResult) {
        List<EventSimilarity> allSimilarities = similarityRepository
                .findTopKSimilarUserEvents(candidateEvents, userEventIds, maxResult);

        return candidateEvents.stream()
                .map(candidateId -> createRecommendation(candidateId, allSimilarities, eventScore))
                .sorted(Comparator.comparingDouble(RecommendedEventProto::getScore).reversed())
                .limit(maxResult)
                .toList();
    }

    private RecommendedEventProto createRecommendation(Long candidateId,
                                                       List<EventSimilarity> allSimilarities,
                                                       Map<Long, Double> eventScore) {
        List<EventSimilarity> relevantSimilarities = filterRelevantSimilarities(candidateId, allSimilarities);
        double predictedScore = calculatePredictedScore(candidateId, relevantSimilarities, eventScore);

        return RecommendedEventProto.newBuilder()
                .setEventId(candidateId)
                .setScore(predictedScore)
                .build();
    }

    private List<EventSimilarity> filterRelevantSimilarities(Long candidateId, List<EventSimilarity> allSimilarities) {
        return allSimilarities.stream()
                .filter(es -> Objects.equals(es.getFirst(), candidateId) ||
                        Objects.equals(es.getSecond(), candidateId))
                .toList();
    }

    private double calculatePredictedScore(Long candidateId,
                                           List<EventSimilarity> similarities,
                                           Map<Long, Double> eventScore) {
        double weightedSum = 0;
        double simSum = 0;

        for (EventSimilarity es : similarities) {
            Long neighborId = getNeighborId(candidateId, es);
            Double neighborScore = eventScore.get(neighborId);

            if (neighborScore != null) {
                weightedSum += neighborScore * es.getScore();
                simSum += es.getScore();
            }
        }

        return simSum > 0 ? weightedSum / simSum : 0;
    }

    private Long getNeighborId(Long candidateId, EventSimilarity similarity) {
        return similarity.getFirst().equals(candidateId) ?
                similarity.getSecond() : similarity.getFirst();
    }

    private void sendRecommendations(List<RecommendedEventProto> recommendations,
                                     StreamObserver<RecommendedEventProto> responseObserver) {
        recommendations.forEach(responseObserver::onNext);
    }
}
