package ru.practicum.config;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ToString
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@ConfigurationProperties("kafka.settings")
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class KafkaSettingsConfig {
    String url;
    String topic;
}