package com.datamanager.common.service;

import com.datamanager.common.exception.ClusterConnectionException;
import com.datamanager.common.exception.DataTransferException;
import com.datamanager.common.model.ClusterConfig;
import com.datamanager.common.model.ClusterInfo;
import com.datamanager.common.model.IndexInfo;

import java.util.List;
import java.util.Map;

/**
 * Interface for cluster operations - implemented by Elasticsearch and OpenSearch services
 */
public interface ClusterService {
    
    /**
     * Test connection to cluster
     */
    boolean testConnection(ClusterConfig config) throws ClusterConnectionException;
    
    /**
     * Get detailed cluster information
     */
    ClusterInfo getClusterInfo(ClusterConfig config) throws ClusterConnectionException;
    
    /**
     * List all indices in the cluster
     */
    List<IndexInfo> listIndices(ClusterConfig config) throws ClusterConnectionException;
    
    /**
     * Export index data to a Map (for cross-cluster transfer)
     */
    Map<String, Object> exportIndexData(ClusterConfig config, String indexName, int batchSize,
                                         boolean includeSettings, boolean includeMappings,
                                         boolean includeAliases) throws DataTransferException;
    
    /**
     * Import index data from a Map (for cross-cluster transfer)
     */
    void importIndexData(ClusterConfig config, Map<String, Object> indexData) throws DataTransferException;
    
    /**
     * Export index data to file
     */
    void exportToFile(ClusterConfig config, List<String> indices, String filePath, 
                      int batchSize, boolean includeSettings, boolean includeMappings,
                      boolean includeAliases) 
            throws DataTransferException;
    
    /**
     * Import data from file to cluster
     */
    void importFromFile(ClusterConfig config, String filePath) throws DataTransferException;
    
    /**
     * Transfer data between clusters (same type only)
     */
    void transferBetweenClusters(ClusterConfig sourceConfig, ClusterConfig targetConfig, 
                                 List<String> indices, int batchSize,
                                 boolean includeSettings, boolean includeMappings,
                                 boolean includeAliases) 
            throws DataTransferException;
    
    /**
     * Create index with settings and mappings
     */
    void createIndex(ClusterConfig config, String indexName, 
                     Map<String, Object> settings, Map<String, Object> mappings) 
            throws ClusterConnectionException;
    
    /**
     * Check if index exists
     */
    boolean indexExists(ClusterConfig config, String indexName) throws ClusterConnectionException;
    
    /**
     * Search documents with pagination
     */
    Map<String, Object> searchDocuments(ClusterConfig config, String indexName, String query, int page, int size) 
            throws ClusterConnectionException;
    
    /**
     * Get single document by ID
     */
    Map<String, Object> getDocument(ClusterConfig config, String indexName, String docId) 
            throws ClusterConnectionException;
    
    /**
     * Delete index
     */
    void deleteIndex(ClusterConfig config, String indexName) throws ClusterConnectionException;
}
