package ru.practicum.kafka;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Service;
import ru.practicum.config.KafkaSettingsConfig;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.service.SimilarityService;

import java.time.Duration;
import java.util.List;


@Slf4j
@Service
@RequiredArgsConstructor
public class SimilarityKafkaService implements Runnable {

    private final SimilarityService service;
    private final KafkaConsumer<String, EventSimilarityAvro> consumer;
    private final KafkaSettingsConfig settingsConfig;

    @Override
    public void run() {
        try {

            consumer.subscribe(List.of(settingsConfig.getSimilarity()));

            while (true) {

                ConsumerRecords<String, EventSimilarityAvro> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, EventSimilarityAvro> datapart : records) {
                    service.save(datapart.value());
                }

                consumer.commitSync();
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {

        } finally {
            try {
                consumer.close();

            } catch (Exception e) {

            }
        }
    }

    @PostConstruct
    public void init() {
        Thread thread = new Thread(this);
        thread.setName("similarity-kafka-service");
        thread.start();
    }

    @PreDestroy
    public void shutdown() {
        consumer.wakeup();
    }
}
