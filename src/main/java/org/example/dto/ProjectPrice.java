package org.example.dto;

import lombok.Data;

@Data
public class ProjectPrice {
    private Long projectId;
    private Long projectCost;
    private String startDate;
    private String finishDate;
}
