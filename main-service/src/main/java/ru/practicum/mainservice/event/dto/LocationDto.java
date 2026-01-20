package ru.practicum.mainservice.event.dto;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocationDto {

    @DecimalMin("-90.0")
    @DecimalMax("90.0")
    private Double lat;

    @DecimalMin("-180.0")
    @DecimalMax("180.0")
    private Double lon;
}