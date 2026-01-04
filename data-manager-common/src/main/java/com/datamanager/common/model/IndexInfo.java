package com.datamanager.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Model representing index information from Elasticsearch/OpenSearch
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexInfo implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String name;
    private long documentCount;
    private String size;
    private int numberOfShards;
    private int numberOfReplicas;
    private String health;
    private String status;
    private boolean selected;
    
    public String getHealthStyle() {
        if (health == null) return "-fx-text-fill: gray;";
        return switch (health.toLowerCase()) {
            case "green" -> "-fx-text-fill: #2e7d32; -fx-font-weight: bold;";
            case "yellow" -> "-fx-text-fill: #f9a825; -fx-font-weight: bold;";
            case "red" -> "-fx-text-fill: #c62828; -fx-font-weight: bold;";
            default -> "-fx-text-fill: gray;";
        };
    }
    
    @Override
    public String toString() {
        return String.format("%s (Docs: %,d, Size: %s)", name, documentCount, size);
    }
}
