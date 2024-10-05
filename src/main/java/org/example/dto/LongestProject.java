package org.example.dto;


import lombok.Data;

@Data
public class LongestProject {
    private Long id;
    private String startDate;
    private String finishDate;
    private Long durationInMonths;
}
