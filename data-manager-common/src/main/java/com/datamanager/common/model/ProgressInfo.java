package com.datamanager.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProgressInfo {
    private String operationId;
    private String currentIndex;
    private int totalIndices;
    private int completedIndices;
    private long totalDocuments;
    private long processedDocuments;
    private String status;
    private String message;
    private double percentage;
    private long bytesProcessed;
    private long totalBytes;
    private long elapsedTimeMs;
    private long estimatedRemainingMs;
    
    public static ProgressInfo of(String operationId, String status, String message, double percentage) {
        return ProgressInfo.builder()
                .operationId(operationId)
                .status(status)
                .message(message)
                .percentage(percentage)
                .build();
    }
}
