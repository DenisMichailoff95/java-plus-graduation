package ru.practicum.service;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.practicum.config.KafkaSettingsConfig;

import ru.practicum.ewm.stats.proto.UserActionProto;
import ru.practicum.mapper.UserActionMapper;

@Slf4j
@Service
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class UserActionHandler implements CollectorHandler<UserActionProto> {

    private final KafkaSettingsConfig kafkaSettingsConfig;
    private final Producer<String, SpecificRecordBase> producer;

    public void handle(UserActionProto proto) {
        try {
            SpecificRecordBase avroRecord = UserActionMapper.mapToAvro(proto);
            ProducerRecord<String, SpecificRecordBase> record =
                    new ProducerRecord<>(kafkaSettingsConfig.getTopic(), avroRecord);

            producer.send(record);
            producer.flush();

            log.debug("Событие успешно отправлено в Kafka: userId={}, eventId={}",
                    proto.getUserId(), proto.getEventId());
        } catch (Exception e) {
            log.error("Ошибка отправки в Kafka: userId={}, eventId={}",
                    proto.getUserId(), proto.getEventId(), e);
            throw new RuntimeException("Failed to send user action to Kafka", e);
        }
    }
}
