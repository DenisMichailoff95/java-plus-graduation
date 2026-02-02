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
import ru.practicum.ewm.stats.avro.UserActionAvro;
import ru.practicum.service.UserActionService;

import java.time.Duration;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserActionKafkaService implements Runnable {

    private final UserActionService service;
    private final KafkaConsumer<String, UserActionAvro> consumer;
    private final KafkaSettingsConfig settingsConfig;

    @Override
    public void run() {
        try {

            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

            consumer.subscribe(List.of(settingsConfig.getAction()));

            while (true) {

                ConsumerRecords<String, UserActionAvro> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, UserActionAvro> datapart : records) {
                    service.save(datapart.value());
                }

                consumer.commitSync();
            }
        } catch (WakeupException ignored) {

        } catch (Exception e) {
            log.error("ошибка обработки: {}", e.getMessage());
        } finally {
            try {
                consumer.close();
                log.info("закрытие консюмер ");
            } catch (Exception e) {
                log.error("ошибка на закрытии консюмера: {}", e.getMessage());
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
