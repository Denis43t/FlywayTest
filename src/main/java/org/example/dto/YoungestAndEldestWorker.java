package org.example.dto;

import lombok.Data;

@Data
public class YoungestAndEldestWorker {
    private String type;
    private String name;
    private Long yearOfBirth;
}
