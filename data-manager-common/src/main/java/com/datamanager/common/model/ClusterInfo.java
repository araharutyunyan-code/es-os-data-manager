package com.datamanager.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Model representing cluster information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterInfo implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String clusterName;
    private String clusterUuid;
    private String version;
    private String luceneVersion;
    private int numberOfNodes;
    private int numberOfDataNodes;
    private String status;
    private long totalIndices;
    private long totalDocuments;
    private String totalSize;
}
