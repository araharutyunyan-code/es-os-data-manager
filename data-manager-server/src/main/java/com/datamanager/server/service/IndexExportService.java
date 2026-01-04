package com.datamanager.server.service;

import com.datamanager.common.model.ClusterConfig;
import com.datamanager.common.service.ClusterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class IndexExportService {

    private final ClusterServiceFactory serviceFactory;

    public Map<String, Object> exportIndex(ClusterConfig config, String indexName, 
                                           int batchSize, boolean includeSettings, 
                                           boolean includeMappings, boolean includeAliases) {
        ClusterService service = serviceFactory.getService(config);
        
        if (service instanceof ElasticsearchService esService) {
            return esService.exportSingleIndex(config, indexName, batchSize, 
                    includeSettings, includeMappings, includeAliases);
        } else if (service instanceof OpenSearchService osService) {
            return osService.exportSingleIndex(config, indexName, batchSize, 
                    includeSettings, includeMappings, includeAliases);
        }
        
        throw new IllegalStateException("Unknown service type");
    }

    public void importIndex(ClusterConfig config, Map<String, Object> indexData) {
        ClusterService service = serviceFactory.getService(config);
        
        if (service instanceof ElasticsearchService esService) {
            esService.importSingleIndex(config, indexData);
        } else if (service instanceof OpenSearchService osService) {
            osService.importSingleIndex(config, indexData);
        } else {
            throw new IllegalStateException("Unknown service type");
        }
    }
}
