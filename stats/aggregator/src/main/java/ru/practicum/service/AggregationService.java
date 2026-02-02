package ru.practicum.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.practicum.ewm.stats.avro.EventSimilarityAvro;
import ru.practicum.ewm.stats.avro.UserActionAvro;
import ru.practicum.config.KafkaSettingsConfig;

import java.time.Duration;
import java.util.List;


@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationService implements Runnable {

    private final UserActionDomainService userActionDomainService;

    private final Consumer<String, UserActionAvro> consumer;
    private final Producer<String, SpecificRecordBase> producer;
    private final KafkaSettingsConfig settingsConfig;

    @Override
    public void run() {
        try {

            log.info("Starting the process of receiving data by the consumer");
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            consumer.subscribe(List.of(settingsConfig.getAction()));
            log.info("topic subscribed: {}", settingsConfig.getAction());

            log.info("Start receiving data from the topic");

            while (true) {
                ConsumerRecords<String, UserActionAvro> records = consumer.poll(Duration.ofMillis(500));
                log.info("Records received: {}", records.count());
                for (ConsumerRecord<String, UserActionAvro> datapart : records) {
                    for (EventSimilarityAvro eventSimilarityAvro : userActionDomainService.calculateSimilarityEvents(datapart.value())) {
                        producer.send(new ProducerRecord<>(settingsConfig.getSimilarity(), datapart.key(), eventSimilarityAvro));
                    }

                    consumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("Processing failed: {}", e.getMessage());
        } finally {
            try {
                producer.flush();
                log.info("Producer buffer sent");
            } catch (Exception e) {
                log.error("Error on final send of producer buffer", e);
            } finally {
                try {
                    producer.close();
                    log.info("producer closed");
                    consumer.close();
                    log.info("consumer closed");
                } catch (Exception e) {
                    log.error("Error closing producer and consumer: {}", e.getMessage());
                }
            }
        }
    }

    @PostConstruct
    public void init() {
        Thread thread = new Thread(this);
        thread.setName("aggregator");
        thread.start();
    }
}


