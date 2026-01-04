package com.datamanager.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Configuration model for Elasticsearch/OpenSearch cluster connection
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterConfig implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String id;
    private String name;
    private String host;
    private int port;
    private String username;
    private String password;
    private boolean useSSL;
    private ClusterType type;
    private String apiKey;
    @Builder.Default
    private int connectionTimeout = 30000;
    @Builder.Default
    private int socketTimeout = 30000;
    
    public enum ClusterType {
        ELASTICSEARCH("Elasticsearch"),
        OPENSEARCH("OpenSearch");
        
        private final String displayName;
        
        ClusterType(String displayName) {
            this.displayName = displayName;
        }
        
        public String getDisplayName() {
            return displayName;
        }
    }
    
    public String getConnectionString() {
        String protocol = useSSL ? "https" : "http";
        return String.format("%s://%s:%d", protocol, host, port);
    }
    
    @Override
    public String toString() {
        return String.format("%s (%s)", name, type.getDisplayName());
    }
}
