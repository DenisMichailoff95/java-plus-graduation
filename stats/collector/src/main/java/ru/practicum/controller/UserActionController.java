package ru.practicum.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import ru.practicum.ewm.stats.proto.UserActionControllerGrpc;
import ru.practicum.ewm.stats.proto.UserActionProto;
import ru.practicum.service.UserActionHandler;

@Slf4j
@GrpcService
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class UserActionController extends UserActionControllerGrpc.UserActionControllerImplBase {

    private final UserActionHandler handler;

    @Override
    public void collectUserAction(UserActionProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("Получен gRPC запрос: userId={}, eventId={}, action={}",
                    request.getUserId(), request.getEventId(), request.getActionType());

            handler.handle(request);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();

            log.debug("gRPC запрос успешно обработан: userId={}, eventId={}",
                    request.getUserId(), request.getEventId());

        } catch (Exception e) {
            log.error("Ошибка обработки gRPC запроса: userId={}, eventId={}",
                    request.getUserId(), request.getEventId(), e);
            responseObserver.onError(
                    new StatusRuntimeException(
                            Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e))
            );
        }
    }
}