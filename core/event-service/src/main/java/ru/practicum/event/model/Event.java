package ru.practicum.event.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import ru.practicum.enums.EventState;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "events")
public class Event {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 120)
    private String title;

    @Column(nullable = false, length = 2000)
    private String annotation;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "category_id", nullable = false)
    private Long categoryId;

    @Column(name = "initiator_id", nullable = false)
    private Long initiatorId;

    @Column(name = "event_date", nullable = false)
    private LocalDateTime eventDate;

    @Builder.Default
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt = LocalDateTime.now();

    @Column(name = "published_on")
    private LocalDateTime publishedOn;

    @Column(name = "location_lat", nullable = false)
    private Double locationLat;

    @Column(name = "location_lon", nullable = false)
    private Double locationLon;

    @Builder.Default
    @Column(nullable = false)
    private Boolean paid = false;

    @Builder.Default
    @Column(name = "participant_limit", columnDefinition = "integer default 0")
    private Integer participantLimit = 0;

    @Builder.Default
    @Column(name = "request_moderation", columnDefinition = "boolean default true")
    private Boolean requestModeration = true;

    @Builder.Default
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventState state = EventState.PENDING;

    @Transient
    @Builder.Default
    private Integer confirmedRequests = 0;

    @Transient
    @Builder.Default
    private Long views = 0L;
}