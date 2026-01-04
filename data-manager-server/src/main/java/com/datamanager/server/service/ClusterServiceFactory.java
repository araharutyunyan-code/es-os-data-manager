package com.datamanager.server.service;

import com.datamanager.common.model.ClusterConfig;
import com.datamanager.common.service.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Factory to get the appropriate ClusterService based on cluster type
 */
@Component
public class ClusterServiceFactory {

    private final ClusterService elasticsearchService;
    private final ClusterService openSearchService;

    @Autowired
    public ClusterServiceFactory(
            @Qualifier("elasticsearchService") ClusterService elasticsearchService,
            @Qualifier("openSearchService") ClusterService openSearchService) {
        this.elasticsearchService = elasticsearchService;
        this.openSearchService = openSearchService;
    }

    public ClusterService getService(ClusterConfig.ClusterType type) {
        return switch (type) {
            case ELASTICSEARCH -> elasticsearchService;
            case OPENSEARCH -> openSearchService;
        };
    }

    public ClusterService getService(ClusterConfig config) {
        return getService(config.getType());
    }
}
