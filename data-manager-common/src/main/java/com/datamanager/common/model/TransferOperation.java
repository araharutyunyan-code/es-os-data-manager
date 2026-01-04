package com.datamanager.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Model representing a data transfer operation with progress tracking
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransferOperation implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private String operationId;
    private TransferType transferType;
    private ClusterConfig sourceCluster;
    private ClusterConfig targetCluster;
    private List<String> indices;
    private String exportFilePath;
    @Builder.Default
    private TransferStatus status = TransferStatus.PENDING;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private long totalDocuments;
    private long processedDocuments;
    private long failedDocuments;
    private String errorMessage;
    @Builder.Default
    private int batchSize = 1000;
    @Builder.Default
    private boolean includeSettings = true;
    @Builder.Default
    private boolean includeMappings = true;
    @Builder.Default
    private boolean includeAliases = true;
    
    public enum TransferType {
        CLUSTER_TO_CLUSTER("Cluster → Cluster"),
        CLUSTER_TO_FILE("Export to File"),
        FILE_TO_CLUSTER("Import from File");
        
        private final String displayName;
        
        TransferType(String displayName) {
            this.displayName = displayName;
        }
        
        public String getDisplayName() {
            return displayName;
        }
    }
    
    public enum TransferStatus {
        PENDING("Pending", "#757575"),
        RUNNING("Running", "#1976d2"),
        PAUSED("Paused", "#ff9800"),
        COMPLETED("Completed", "#2e7d32"),
        FAILED("Failed", "#c62828"),
        CANCELLED("Cancelled", "#9e9e9e");
        
        private final String displayName;
        private final String color;
        
        TransferStatus(String displayName, String color) {
            this.displayName = displayName;
            this.color = color;
        }
        
        public String getDisplayName() {
            return displayName;
        }
        
        public String getColor() {
            return color;
        }
    }
    
    public double getProgress() {
        if (totalDocuments == 0) return 0;
        return (double) processedDocuments / totalDocuments * 100;
    }
    
    public String getFormattedStartTime() {
        return startTime != null ? startTime.format(FORMATTER) : "-";
    }
    
    public String getFormattedEndTime() {
        return endTime != null ? endTime.format(FORMATTER) : "-";
    }
    
    public String getDuration() {
        if (startTime == null) return "-";
        LocalDateTime end = endTime != null ? endTime : LocalDateTime.now();
        long seconds = java.time.Duration.between(startTime, end).getSeconds();
        if (seconds < 60) return seconds + "s";
        if (seconds < 3600) return (seconds / 60) + "m " + (seconds % 60) + "s";
        return (seconds / 3600) + "h " + ((seconds % 3600) / 60) + "m";
    }
}
