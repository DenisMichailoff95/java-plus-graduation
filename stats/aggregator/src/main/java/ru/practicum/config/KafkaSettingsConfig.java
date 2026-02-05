package ru.practicum.config;

import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ToString
@ConfigurationProperties(prefix = "kafka.settings")
public class KafkaSettingsConfig {
    private String url;
    private String action;
    private String similarity;
}
