package ru.practicum.compilation.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "compilations")
public class Compilation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 55)
    private String title;

    @Column(nullable = false)
    private Boolean pinned;

    @ElementCollection
    @CollectionTable(name = "compilation_events",
            joinColumns = @JoinColumn(name = "compilation_id"))
    @Column(name = "event_id")
    @Builder.Default
    private Set<Long> eventIds = new HashSet<>();
}