package ru.practicum.config;

import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ToString
@FieldDefaults(level = AccessLevel.PRIVATE)
@ConfigurationProperties("kafka.settings")
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public class KafkaSettingsConfig {
    String url;
    String action;
    String similarity;
}
