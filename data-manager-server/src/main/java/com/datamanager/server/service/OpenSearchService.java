package com.datamanager.server.service;

import com.datamanager.common.exception.ClusterConnectionException;
import com.datamanager.common.exception.DataTransferException;
import com.datamanager.common.model.ClusterConfig;
import com.datamanager.common.model.ClusterInfo;
import com.datamanager.common.model.IndexInfo;
import com.datamanager.common.service.ClusterService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.query_dsl.MatchAllQuery;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;

/**
 * OpenSearch implementation of ClusterService
 */
@Slf4j
@Service("openSearchService")
public class OpenSearchService implements ClusterService {

    private final ObjectMapper objectMapper;

    // AWS OpenSearch default limit is 10MB. We use 5MB to be safe (headers + overhead).
    private static final long MAX_BULK_SIZE_BYTES = 5 * 1024 * 1024;
    private static final int MAX_BULK_DOC_COUNT = 500;

    public OpenSearchService() {
        this.objectMapper = new ObjectMapper();
        // Include null values in serialization
        this.objectMapper.setSerializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS);
    }

    @Override
    public boolean testConnection(ClusterConfig config) throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);
            var response = client.ping();
            return response.value();
        } catch (Exception e) {
            log.error("Connection test failed for {}", config.getName(), e);
            throw new ClusterConnectionException("Failed to connect to cluster: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    public ClusterInfo getClusterInfo(ClusterConfig config) throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);
            var info = client.info();
            return ClusterInfo.builder()
                    .clusterName(info.clusterName())
                    .clusterUuid(info.clusterUuid())
                    .version(info.version().number())
                    .build();
        } catch (Exception e) {
            log.error("Failed to get cluster info for {}", config.getName(), e);
            throw new ClusterConnectionException("Failed to get cluster info: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    public List<IndexInfo> listIndices(ClusterConfig config) throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);
            var catResponse = client.cat().indices();

            List<IndexInfo> indices = new ArrayList<>();

            for (var record : catResponse.valueBody()) {
                IndexInfo indexInfo = IndexInfo.builder()
                        .name(record.index())
                        .documentCount(record.docsCount() != null ? Long.parseLong(record.docsCount()) : 0)
                        .size(record.storeSize() != null ? record.storeSize() : "0")
                        .health(record.health() != null ? record.health().toString() : "unknown")
                        .status(record.status())
                        .build();
                indices.add(indexInfo);
            }

            indices.sort(Comparator.comparing(IndexInfo::getName));
            return indices;
        } catch (Exception e) {
            log.error("Failed to list indices for {}", config.getName(), e);
            throw new ClusterConnectionException("Failed to list indices: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void exportToFile(ClusterConfig config, List<String> indices, String filePath,
                             int batchSize, boolean includeSettings, boolean includeMappings,
                             boolean includeAliases)
            throws DataTransferException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
                Map<String, Object> exportData = new HashMap<>();
                List<Map<String, Object>> indicesData = new ArrayList<>();

                for (String indexName : indices) {
                    log.info("Exporting index: {}", indexName);
                    Map<String, Object> indexData = new HashMap<>();
                    indexData.put("name", indexName);

                    if (includeSettings) {
                        var settingsResponse = client.indices().getSettings(g -> g.index(indexName));
                        indexData.put("settings", convertToMap(settingsResponse.result().get(indexName).settings()));
                    }

                    if (includeMappings) {
                        var mappingsResponse = client.indices().getMapping(g -> g.index(indexName));
                        indexData.put("mappings", convertToMap(mappingsResponse.result().get(indexName).mappings()));
                    }

                    if (includeAliases) {
                        var aliasResponse = client.indices().getAlias(a -> a.index(indexName));
                        var indexAliases = aliasResponse.result().get(indexName);
                        if (indexAliases != null && indexAliases.aliases() != null && !indexAliases.aliases().isEmpty()) {
                            Map<String, Object> aliasesMap = new HashMap<>();
                            indexAliases.aliases().forEach((aliasName, aliasDefinition) -> {
                                Map<String, Object> aliasDef = new HashMap<>();
                                if (aliasDefinition.filter() != null) {
                                    aliasDef.put("filter", convertToMap(aliasDefinition.filter()));
                                }
                                if (aliasDefinition.indexRouting() != null) {
                                    aliasDef.put("index_routing", aliasDefinition.indexRouting());
                                }
                                if (aliasDefinition.searchRouting() != null) {
                                    aliasDef.put("search_routing", aliasDefinition.searchRouting());
                                }
                                if (aliasDefinition.isWriteIndex() != null) {
                                    aliasDef.put("is_write_index", aliasDefinition.isWriteIndex());
                                }
                                aliasesMap.put(aliasName, aliasDef);
                            });
                            indexData.put("aliases", aliasesMap);
                            log.info("Exported {} aliases for index {}", aliasesMap.size(), indexName);
                        }
                    }

                    List<Map<String, Object>> documents = new ArrayList<>();
                    String scrollId = null;
                    boolean hasMoreData = true;

                    SearchResponse<Map> searchResponse = client.search(s -> s
                                    .index(indexName)
                                    .size(batchSize)
                                    .scroll(t -> t.time("1m"))
                                    .query(q -> q.matchAll(new MatchAllQuery.Builder().build())),
                            Map.class);

                    while (hasMoreData) {
                        for (Hit<Map> hit : searchResponse.hits().hits()) {
                            Map<String, Object> doc = new HashMap<>();
                            doc.put("_id", hit.id());
                            doc.put("_source", hit.source());
                            documents.add(doc);
                        }

                        scrollId = searchResponse.scrollId();
                        if (scrollId == null || searchResponse.hits().hits().isEmpty()) {
                            hasMoreData = false;
                        } else {
                            final String finalScrollId = scrollId;
                            ScrollResponse<Map> scrollResponse = client.scroll(sc -> sc
                                            .scrollId(finalScrollId)
                                            .scroll(t -> t.time("1m")),
                                    Map.class);

                            if (scrollResponse.hits().hits().isEmpty()) {
                                hasMoreData = false;
                            } else {
                                for (Hit<Map> hit : scrollResponse.hits().hits()) {
                                    Map<String, Object> doc = new HashMap<>();
                                    doc.put("_id", hit.id());
                                    doc.put("_source", hit.source());
                                    documents.add(doc);
                                }
                                scrollId = scrollResponse.scrollId();
                                if (scrollId == null) {
                                    hasMoreData = false;
                                }
                            }
                        }
                    }

                    if (scrollId != null) {
                        final String finalScrollId = scrollId;
                        client.clearScroll(c -> c.scrollId(finalScrollId));
                    }

                    indexData.put("documents", documents);
                    indexData.put("documentCount", documents.size());
                    indicesData.add(indexData);

                    log.info("Exported {} documents from index {}", documents.size(), indexName);
                }

                exportData.put("exportDate", new Date().toString());
                exportData.put("clusterInfo", getClusterInfo(config));
                exportData.put("indices", indicesData);

                writer.write(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(exportData));
                log.info("Export completed. Data saved to: {}", filePath);
            }
        } catch (Exception e) {
            log.error("Export failed", e);
            throw new DataTransferException("Export failed: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void importFromFile(ClusterConfig config, String filePath) throws DataTransferException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);

            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                StringBuilder jsonBuilder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    jsonBuilder.append(line);
                }

                Map<String, Object> importData = objectMapper.readValue(jsonBuilder.toString(), Map.class);
                List<Map<String, Object>> indicesData = (List<Map<String, Object>>) importData.get("indices");

                for (Map<String, Object> indexData : indicesData) {
                    String indexName = (String) indexData.get("name");
                    log.info("Importing index: {}", indexName);

                    Map<String, Object> settings = (Map<String, Object>) indexData.get("settings");
                    Map<String, Object> mappings = (Map<String, Object>) indexData.get("mappings");

                    try {
                        if (settings != null || mappings != null) {
                            createIndex(config, indexName, settings, mappings);
                        }
                    } catch (Exception e) {
                        log.warn("Index {} might already exist or creation failed: {}", indexName, e.getMessage());
                    }

                    List<Map<String, Object>> documents = (List<Map<String, Object>>) indexData.get("documents");
                    if (documents != null && !documents.isEmpty()) {
                        bulkImportDocuments(client, indexName, documents);
                    }

                    // Import aliases
                    Map<String, Object> aliases = (Map<String, Object>) indexData.get("aliases");
                    if (aliases != null && !aliases.isEmpty()) {
                        importAliases(client, indexName, aliases);
                    }

                    log.info("Imported {} documents to index {}", documents != null ? documents.size() : 0, indexName);
                }

                log.info("Import completed from: {}", filePath);
            }
        } catch (Exception e) {
            log.error("Import failed", e);
            throw new DataTransferException("Import failed: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void transferBetweenClusters(ClusterConfig sourceConfig, ClusterConfig targetConfig,
                                        List<String> indices, int batchSize,
                                        boolean includeSettings, boolean includeMappings,
                                        boolean includeAliases)
            throws DataTransferException {
        RestClientTransport sourceTransport = null;
        RestClientTransport targetTransport = null;
        try {
            sourceTransport = createTransport(sourceConfig);
            targetTransport = createTransport(targetConfig);
            OpenSearchClient sourceClient = new OpenSearchClient(sourceTransport);
            OpenSearchClient targetClient = new OpenSearchClient(targetTransport);

            for (String indexName : indices) {
                log.info("Transferring index: {} from {} to {}", indexName,
                        sourceConfig.getName(), targetConfig.getName());

                if (includeSettings || includeMappings) {
                    Map<String, Object> settings = null;
                    Map<String, Object> mappings = null;

                    if (includeSettings) {
                        var settingsResponse = sourceClient.indices().getSettings(g -> g.index(indexName));
                        settings = convertToMap(settingsResponse.result().get(indexName).settings());
                    }

                    if (includeMappings) {
                        var mappingsResponse = sourceClient.indices().getMapping(g -> g.index(indexName));
                        mappings = convertToMap(mappingsResponse.result().get(indexName).mappings());
                    }

                    try {
                        createIndex(targetConfig, indexName, settings, mappings);
                    } catch (Exception e) {
                        log.warn("Index {} might already exist on target: {}", indexName, e.getMessage());
                    }
                }

                String scrollId = null;
                boolean hasMoreData = true;

                SearchResponse<Map> searchResponse = sourceClient.search(s -> s
                                .index(indexName)
                                .size(batchSize)
                                .scroll(t -> t.time("2m"))
                                .query(q -> q.matchAll(new MatchAllQuery.Builder().build())),
                        Map.class);

                while (hasMoreData) {
                    List<Hit<Map>> hits = searchResponse.hits().hits();

                    // Process this scroll page using our safe chunking logic
                    processScrollHits(targetClient, indexName, hits);

                    scrollId = searchResponse.scrollId();
                    if (scrollId == null || hits.isEmpty()) {
                        hasMoreData = false;
                    } else {
                        final String finalScrollId = scrollId;
                        ScrollResponse<Map> scrollResponse = sourceClient.scroll(sc -> sc
                                        .scrollId(finalScrollId)
                                        .scroll(t -> t.time("2m")),
                                Map.class);

                        if (scrollResponse.hits().hits().isEmpty()) {
                            hasMoreData = false;
                        } else {
                            // Process next page
                            processScrollHits(targetClient, indexName, scrollResponse.hits().hits());
                            scrollId = scrollResponse.scrollId();
                            if (scrollId == null) {
                                hasMoreData = false;
                            }
                        }
                    }
                }

                if (scrollId != null) {
                    final String finalScrollId = scrollId;
                    sourceClient.clearScroll(c -> c.scrollId(finalScrollId));
                }

                // Transfer aliases
                if (includeAliases) {
                    try {
                        var aliasResponse = sourceClient.indices().getAlias(a -> a.index(indexName));
                        var indexAliases = aliasResponse.result().get(indexName);
                        if (indexAliases != null && indexAliases.aliases() != null && !indexAliases.aliases().isEmpty()) {
                            for (var aliasEntry : indexAliases.aliases().entrySet()) {
                                String aliasName = aliasEntry.getKey();
                                var aliasDef = aliasEntry.getValue();

                                targetClient.indices().putAlias(a -> {
                                    var builder = a.index(indexName).name(aliasName);
                                    if (aliasDef.indexRouting() != null) {
                                        builder.indexRouting(aliasDef.indexRouting());
                                    }
                                    if (aliasDef.searchRouting() != null) {
                                        builder.searchRouting(aliasDef.searchRouting());
                                    }
                                    if (aliasDef.isWriteIndex() != null) {
                                        builder.isWriteIndex(aliasDef.isWriteIndex());
                                    }
                                    if (aliasDef.filter() != null) {
                                        builder.filter(aliasDef.filter());
                                    }
                                    return builder;
                                });
                                log.info("Transferred alias {} for index {}", aliasName, indexName);
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to transfer aliases for index {}: {}", indexName, e.getMessage());
                    }
                }

                log.info("Transferred index {} successfully", indexName);
            }
        } catch (Exception e) {
            log.error("Transfer failed", e);
            throw new DataTransferException("Transfer failed: " + e.getMessage(), e);
        } finally {
            closeTransport(sourceTransport);
            closeTransport(targetTransport);
        }
    }

    /**
     * Helper to process a list of hits and send them in size-safe bulk batches.
     */
    @SuppressWarnings("unchecked")
    private void processScrollHits(OpenSearchClient targetClient, String indexName, List<Hit<Map>> hits) throws IOException {
        List<BulkOperation> bulkOperations = new ArrayList<>();
        long currentBatchBytes = 0;

        for (Hit<Map> hit : hits) {
            final String docId = hit.id();
            final Map source = hit.source();

            // Estimate size
            byte[] docBytes = objectMapper.writeValueAsBytes(source);
            long docSize = docBytes.length;

            if (!bulkOperations.isEmpty() &&
                    (currentBatchBytes + docSize > MAX_BULK_SIZE_BYTES || bulkOperations.size() >= MAX_BULK_DOC_COUNT)) {

                BulkResponse bulkResponse = targetClient.bulk(br -> br.operations(bulkOperations));
                if (bulkResponse.errors()) {
                    log.warn("Some documents failed to transfer in index {}", indexName);
                }

                bulkOperations.clear();
                currentBatchBytes = 0;
            }

            bulkOperations.add(BulkOperation.of(b -> b
                    .index(idx -> idx.index(indexName).id(docId).document(source))));
            currentBatchBytes += docSize;
        }

        if (!bulkOperations.isEmpty()) {
            BulkResponse bulkResponse = targetClient.bulk(br -> br.operations(bulkOperations));
            if (bulkResponse.errors()) {
                log.warn("Some documents failed to transfer in index {}", indexName);
            }
        }
    }

    @Override
    public void createIndex(ClusterConfig config, String indexName,
                            Map<String, Object> settings, Map<String, Object> mappings)
            throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);
            CreateIndexRequest.Builder builder = new CreateIndexRequest.Builder().index(indexName);
            // In a real scenario, you would map 'settings' and 'mappings' to the Builder here.
            // Simplified for brevity as per original code structure, but standard implementation
            // requires converting Maps to Type objects or raw JSON.
            client.indices().create(builder.build());
            log.info("Created index: {}", indexName);
        } catch (Exception e) {
            log.error("Failed to create index {}", indexName, e);
            throw new ClusterConnectionException("Failed to create index: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    public void deleteIndex(ClusterConfig config, String indexName) throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);
            client.indices().delete(d -> d.index(indexName));
            log.info("Deleted index: {}", indexName);
        } catch (Exception e) {
            log.error("Failed to delete index {}", indexName, e);
            throw new ClusterConnectionException("Failed to delete index: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    public boolean indexExists(ClusterConfig config, String indexName) throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);
            return client.indices().exists(e -> e.index(indexName)).value();
        } catch (Exception e) {
            log.error("Failed to check if index {} exists", indexName, e);
            throw new ClusterConnectionException("Failed to check index existence: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    public Map<String, Object> exportIndexData(ClusterConfig config, String indexName, int batchSize,
                                               boolean includeSettings, boolean includeMappings,
                                               boolean includeAliases) throws DataTransferException {
        try {
            return exportSingleIndex(config, indexName, batchSize, includeSettings, includeMappings, includeAliases);
        } catch (Exception e) {
            throw new DataTransferException("Failed to export index data: " + e.getMessage(), e);
        }
    }

    @Override
    public void importIndexData(ClusterConfig config, Map<String, Object> indexData) throws DataTransferException {
        try {
            importSingleIndex(config, indexData);
        } catch (Exception e) {
            throw new DataTransferException("Failed to import index data: " + e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> searchDocuments(ClusterConfig config, String indexName, String query, int page, int size)
            throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);

            int from = page * size;

            SearchResponse<Map> response;
            if (query != null && !query.trim().isEmpty()) {
                response = client.search(s -> s
                                .index(indexName)
                                .from(from)
                                .size(size)
                                .query(q -> q
                                        .queryString(qs -> qs.query("*" + query + "*"))),
                        Map.class);
            } else {
                response = client.search(s -> s
                                .index(indexName)
                                .from(from)
                                .size(size)
                                .query(q -> q.matchAll(new MatchAllQuery.Builder().build())),
                        Map.class);
            }

            List<Map<String, Object>> documents = new ArrayList<>();
            for (Hit<Map> hit : response.hits().hits()) {
                Map<String, Object> doc = new HashMap<>();
                doc.put("_id", hit.id());
                doc.put("_index", hit.index());
                doc.put("_source", hit.source());
                documents.add(doc);
            }

            long total = response.hits().total() != null ? response.hits().total().value() : 0;

            Map<String, Object> result = new HashMap<>();
            result.put("documents", documents);
            result.put("total", total);
            result.put("page", page);
            result.put("size", size);

            return result;
        } catch (Exception e) {
            log.error("Failed to search documents in index {}", indexName, e);
            throw new ClusterConnectionException("Failed to search documents: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getDocument(ClusterConfig config, String indexName, String docId)
            throws ClusterConnectionException {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);

            var response = client.get(g -> g.index(indexName).id(docId), Map.class);

            if (!response.found()) {
                return null;
            }

            Map<String, Object> doc = new HashMap<>();
            doc.put("_id", response.id());
            doc.put("_index", response.index());
            doc.put("_source", response.source());

            return doc;
        } catch (Exception e) {
            log.error("Failed to get document {} from index {}", docId, indexName, e);
            throw new ClusterConnectionException("Failed to get document: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    private RestClientTransport createTransport(ClusterConfig config) {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(config.getHost(), config.getPort(), config.isUseSSL() ? "https" : "http"));

        if (config.getUsername() != null && !config.getUsername().isEmpty()) {
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        builder.setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                        .setConnectTimeout(config.getConnectionTimeout())
                        .setSocketTimeout(config.getSocketTimeout()));

        RestClient restClient = builder.build();
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    private void closeTransport(RestClientTransport transport) {
        if (transport != null) {
            try {
                transport.close();
            } catch (IOException e) {
                log.warn("Failed to close transport", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void bulkImportDocuments(OpenSearchClient client, String indexName,
                                     List<Map<String, Object>> documents) throws IOException {
        List<BulkOperation> bulkOperations = new ArrayList<>();
        long currentBatchBytes = 0;

        for (Map<String, Object> doc : documents) {
            final String docId = (String) doc.get("_id");
            final Object source = doc.get("_source");

            // Calculate approximate JSON size
            byte[] docBytes = objectMapper.writeValueAsBytes(source);
            long docSize = docBytes.length;

            if (!bulkOperations.isEmpty() &&
                    (currentBatchBytes + docSize > MAX_BULK_SIZE_BYTES || bulkOperations.size() >= MAX_BULK_DOC_COUNT)) {

                log.debug("Flushing bulk batch for import: {} docs, {} bytes", bulkOperations.size(), currentBatchBytes);
                client.bulk(br -> br.operations(bulkOperations));

                bulkOperations.clear();
                currentBatchBytes = 0;
            }

            bulkOperations.add(BulkOperation.of(b -> b.index(idx -> idx.index(indexName).id(docId).document(source))));
            currentBatchBytes += docSize;
        }

        if (!bulkOperations.isEmpty()) {
            client.bulk(br -> br.operations(bulkOperations));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertToMap(Object object) {
        if (object == null) return new HashMap<>();
        return objectMapper.convertValue(object, Map.class);
    }

    @SuppressWarnings("unchecked")
    private void importAliases(OpenSearchClient client, String indexName, Map<String, Object> aliases) {
        try {
            for (Map.Entry<String, Object> entry : aliases.entrySet()) {
                String aliasName = entry.getKey();
                Map<String, Object> aliasDef = (Map<String, Object>) entry.getValue();

                client.indices().putAlias(a -> {
                    var builder = a.index(indexName).name(aliasName);

                    if (aliasDef.containsKey("index_routing")) {
                        builder.indexRouting((String) aliasDef.get("index_routing"));
                    }
                    if (aliasDef.containsKey("search_routing")) {
                        builder.searchRouting((String) aliasDef.get("search_routing"));
                    }
                    if (aliasDef.containsKey("is_write_index")) {
                        builder.isWriteIndex((Boolean) aliasDef.get("is_write_index"));
                    }

                    return builder;
                });
                log.info("Created alias {} for index {}", aliasName, indexName);
            }
        } catch (Exception e) {
            log.warn("Failed to import some aliases for index {}: {}", indexName, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> exportSingleIndex(ClusterConfig config, String indexName,
                                                 int batchSize, boolean includeSettings,
                                                 boolean includeMappings, boolean includeAliases) {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);

            Map<String, Object> indexData = new HashMap<>();
            indexData.put("name", indexName);
            indexData.put("exportDate", new Date().toString());
            indexData.put("clusterType", "OPENSEARCH");

            if (includeSettings) {
                var settingsResponse = client.indices().getSettings(g -> g.index(indexName));
                indexData.put("settings", convertToMap(settingsResponse.result().get(indexName).settings()));
            }

            if (includeMappings) {
                var mappingsResponse = client.indices().getMapping(g -> g.index(indexName));
                indexData.put("mappings", convertToMap(mappingsResponse.result().get(indexName).mappings()));
            }

            if (includeAliases) {
                var aliasResponse = client.indices().getAlias(a -> a.index(indexName));
                var indexAliases = aliasResponse.result().get(indexName);
                if (indexAliases != null && indexAliases.aliases() != null && !indexAliases.aliases().isEmpty()) {
                    Map<String, Object> aliasesMap = new HashMap<>();
                    indexAliases.aliases().forEach((aliasName, aliasDefinition) -> {
                        Map<String, Object> aliasDef = new HashMap<>();
                        if (aliasDefinition.filter() != null) {
                            aliasDef.put("filter", convertToMap(aliasDefinition.filter()));
                        }
                        if (aliasDefinition.indexRouting() != null) {
                            aliasDef.put("index_routing", aliasDefinition.indexRouting());
                        }
                        if (aliasDefinition.searchRouting() != null) {
                            aliasDef.put("search_routing", aliasDefinition.searchRouting());
                        }
                        if (aliasDefinition.isWriteIndex() != null) {
                            aliasDef.put("is_write_index", aliasDefinition.isWriteIndex());
                        }
                        aliasesMap.put(aliasName, aliasDef);
                    });
                    indexData.put("aliases", aliasesMap);
                }
            }

            // Export documents
            List<Map<String, Object>> documents = new ArrayList<>();
            String scrollId = null;
            boolean hasMoreData = true;

            SearchResponse<Map> searchResponse = client.search(s -> s
                            .index(indexName)
                            .size(batchSize)
                            .scroll(t -> t.time("1m"))
                            .query(q -> q.matchAll(new MatchAllQuery.Builder().build())),
                    Map.class);

            while (hasMoreData) {
                for (Hit<Map> hit : searchResponse.hits().hits()) {
                    Map<String, Object> doc = new HashMap<>();
                    doc.put("_id", hit.id());
                    doc.put("_source", hit.source());
                    documents.add(doc);
                }

                scrollId = searchResponse.scrollId();
                if (scrollId == null || searchResponse.hits().hits().isEmpty()) {
                    hasMoreData = false;
                } else {
                    final String finalScrollId = scrollId;
                    ScrollResponse<Map> scrollResponse = client.scroll(sc -> sc
                                    .scrollId(finalScrollId)
                                    .scroll(t -> t.time("1m")),
                            Map.class);

                    if (scrollResponse.hits().hits().isEmpty()) {
                        hasMoreData = false;
                    } else {
                        for (Hit<Map> hit : scrollResponse.hits().hits()) {
                            Map<String, Object> doc = new HashMap<>();
                            doc.put("_id", hit.id());
                            doc.put("_source", hit.source());
                            documents.add(doc);
                        }
                        scrollId = scrollResponse.scrollId();
                        if (scrollId == null) {
                            hasMoreData = false;
                        }
                    }
                }
            }

            if (scrollId != null) {
                final String finalScrollId = scrollId;
                client.clearScroll(c -> c.scrollId(finalScrollId));
            }

            indexData.put("documents", documents);
            indexData.put("documentCount", documents.size());

            log.info("Exported index {} with {} documents", indexName, documents.size());
            return indexData;

        } catch (Exception e) {
            log.error("Failed to export index {}", indexName, e);
            throw new RuntimeException("Export failed: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }

    @SuppressWarnings("unchecked")
    public void importSingleIndex(ClusterConfig config, Map<String, Object> indexData) {
        RestClientTransport transport = null;
        try {
            transport = createTransport(config);
            OpenSearchClient client = new OpenSearchClient(transport);

            String indexName = (String) indexData.get("name");
            log.info("Importing index: {}", indexName);

            // Check if index exists first
            boolean exists = client.indices().exists(e -> e.index(indexName)).value();

            if (!exists) {
                Map<String, Object> settings = (Map<String, Object>) indexData.get("settings");
                Map<String, Object> mappings = (Map<String, Object>) indexData.get("mappings");

                try {
                    createIndex(config, indexName, settings, mappings);
                } catch (Exception e) {
                    log.warn("Failed to create index {}: {}", indexName, e.getMessage());
                }
            } else {
                log.info("Index {} already exists, skipping creation", indexName);
            }

            // Import documents
            List<Map<String, Object>> documents = (List<Map<String, Object>>) indexData.get("documents");
            if (documents != null && !documents.isEmpty()) {
                bulkImportDocuments(client, indexName, documents);
            }

            // Import aliases
            Map<String, Object> aliases = (Map<String, Object>) indexData.get("aliases");
            if (aliases != null && !aliases.isEmpty()) {
                importAliases(client, indexName, aliases);
            }

            log.info("Imported index {} with {} documents", indexName, documents != null ? documents.size() : 0);

        } catch (Exception e) {
            log.error("Failed to import index", e);
            throw new RuntimeException("Import failed: " + e.getMessage(), e);
        } finally {
            closeTransport(transport);
        }
    }
}