package com.datamanager.server.controller;

import com.datamanager.common.model.*;
import com.datamanager.common.service.ClusterService;
import com.datamanager.server.entity.ClusterConfigEntity;
import com.datamanager.server.repository.ClusterConfigRepository;
import com.datamanager.server.service.ClusterServiceFactory;
import com.datamanager.server.service.DataTransferService;
import com.datamanager.server.service.IndexExportService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class DataManagerController {

    private final ClusterServiceFactory serviceFactory;
    private final DataTransferService dataTransferService;
    private final ClusterConfigRepository clusterRepository;
    private final IndexExportService indexExportService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // SSE emitters for progress updates
    private final CopyOnWriteArrayList<SseEmitter> progressEmitters = new CopyOnWriteArrayList<>();
    private final Map<String, ProgressInfo> operationProgress = new ConcurrentHashMap<>();

    @GetMapping("/clusters")
    public ResponseEntity<List<ClusterConfig>> getClusters() {
        List<ClusterConfig> clusters = clusterRepository.findAll()
                .stream()
                .map(ClusterConfigEntity::toClusterConfig)
                .collect(Collectors.toList());
        return ResponseEntity.ok(clusters);
    }

    @PostMapping("/clusters")
    public ResponseEntity<?> addCluster(@RequestBody ClusterConfig config) {
        if (config.getName() == null || config.getName().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Name is required"));
        }
        if (config.getHost() == null || config.getHost().trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Host is required"));
        }
        if (config.getPort() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("error", "Valid port is required"));
        }
        if (config.getType() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Cluster type is required"));
        }

        try {
            ClusterService service = serviceFactory.getService(config);
            if (!service.testConnection(config)) {
                return ResponseEntity.badRequest().body(Map.of("error", "Connection test failed - cluster not responding"));
            }
        } catch (Exception e) {
            log.error("Connection test failed for new cluster: {} at {}:{}", config.getName(), config.getHost(), config.getPort(), e);
            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.contains("Connection refused")) {
                errorMsg = "Connection refused - check if " + config.getType() + " is running on " + config.getHost() + ":" + config.getPort();
            } else if (errorMsg != null && errorMsg.contains("UnknownHostException")) {
                errorMsg = "Unknown host: " + config.getHost();
            } else if (errorMsg != null && errorMsg.contains("timeout")) {
                errorMsg = "Connection timeout - check network and firewall settings";
            }
            return ResponseEntity.badRequest().body(Map.of("error", errorMsg != null ? errorMsg : "Connection failed"));
        }

        config.setId(UUID.randomUUID().toString());
        ClusterConfigEntity entity = ClusterConfigEntity.fromClusterConfig(config);
        clusterRepository.save(entity);
        log.info("Added cluster: {} ({}:{})", config.getName(), config.getHost(), config.getPort());
        return ResponseEntity.ok(config);
    }

    @DeleteMapping("/clusters/{id}")
    @Transactional
    public ResponseEntity<Void> deleteCluster(@PathVariable("id") String id) {
        if (clusterRepository.existsById(id)) {
            clusterRepository.deleteById(id);
            clusterRepository.flush();
            log.info("Deleted cluster: {}", id);
            return ResponseEntity.ok().build();
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("/clusters/{id}/test")
    public ResponseEntity<?> testConnection(@PathVariable("id") String id) {
        Optional<ClusterConfigEntity> entity = clusterRepository.findById(id);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        ClusterConfig config = entity.get().toClusterConfig();
        try {
            ClusterService service = serviceFactory.getService(config);
            if (service.testConnection(config)) {
                ClusterInfo info = service.getClusterInfo(config);
                return ResponseEntity.ok(info);
            }
            return ResponseEntity.badRequest().body(Map.of("error", "Connection failed"));
        } catch (Exception e) {
            log.error("Connection test failed", e);
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/clusters/{id}/indices")
    public ResponseEntity<List<IndexInfo>> getIndices(@PathVariable("id") String id) {
        Optional<ClusterConfigEntity> entity = clusterRepository.findById(id);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        ClusterConfig config = entity.get().toClusterConfig();
        try {
            ClusterService service = serviceFactory.getService(config);
            List<IndexInfo> indices = service.listIndices(config);
            return ResponseEntity.ok(indices);
        } catch (Exception e) {
            log.error("Failed to list indices", e);
            return ResponseEntity.badRequest().build();
        }
    }

    // SSE endpoint for real-time progress updates
    @GetMapping(value = "/progress", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamProgress() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        progressEmitters.add(emitter);
        
        emitter.onCompletion(() -> progressEmitters.remove(emitter));
        emitter.onTimeout(() -> progressEmitters.remove(emitter));
        emitter.onError(e -> progressEmitters.remove(emitter));
        
        // Send current state of all operations
        try {
            for (ProgressInfo info : operationProgress.values()) {
                emitter.send(SseEmitter.event().name("progress").data(info));
            }
        } catch (Exception e) {
            log.warn("Failed to send initial progress", e);
        }
        
        return emitter;
    }

    private void broadcastProgress(ProgressInfo progress) {
        operationProgress.put(progress.getOperationId(), progress);
        
        List<SseEmitter> deadEmitters = new ArrayList<>();
        for (SseEmitter emitter : progressEmitters) {
            try {
                emitter.send(SseEmitter.event().name("progress").data(progress));
            } catch (Exception e) {
                deadEmitters.add(emitter);
            }
        }
        progressEmitters.removeAll(deadEmitters);
    }

    @PostMapping("/transfer")
    public ResponseEntity<?> startTransfer(@RequestBody TransferRequest request) {
        Optional<ClusterConfigEntity> sourceEntity = clusterRepository.findById(request.sourceClusterId);
        Optional<ClusterConfigEntity> targetEntity = clusterRepository.findById(request.targetClusterId);

        if (sourceEntity.isEmpty() || targetEntity.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid cluster ID"));
        }

        ClusterConfig source = sourceEntity.get().toClusterConfig();
        ClusterConfig target = targetEntity.get().toClusterConfig();

        String operationId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        TransferOperation op = TransferOperation.builder()
                .operationId(operationId)
                .transferType(TransferOperation.TransferType.CLUSTER_TO_CLUSTER)
                .sourceCluster(source)
                .targetCluster(target)
                .indices(request.indices)
                .batchSize(request.batchSize)
                .includeSettings(request.includeSettings)
                .includeMappings(request.includeMappings)
                .includeAliases(request.includeAliases)
                .build();

        dataTransferService.executeTransfer(op, progress -> broadcastProgress(progress));
        
        return ResponseEntity.ok(Map.of(
            "operationId", operationId,
            "message", "Transfer started",
            "indices", request.indices.size()
        ));
    }

    // Export single index with progress
    @GetMapping("/clusters/{clusterId}/indices/{indexName}/export")
    public ResponseEntity<byte[]> exportSingleIndex(
            @PathVariable("clusterId") String clusterId,
            @PathVariable("indexName") String indexName,
            @RequestParam(value = "includeSettings", defaultValue = "true") boolean includeSettings,
            @RequestParam(value = "includeMappings", defaultValue = "true") boolean includeMappings,
            @RequestParam(value = "includeAliases", defaultValue = "true") boolean includeAliases,
            @RequestParam(value = "batchSize", defaultValue = "1000") int batchSize) {

        Optional<ClusterConfigEntity> entity = clusterRepository.findById(clusterId);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String operationId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        try {
            ClusterConfig config = entity.get().toClusterConfig();
            
            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("RUNNING")
                    .message("Exporting " + indexName)
                    .currentIndex(indexName)
                    .totalIndices(1)
                    .completedIndices(0)
                    .percentage(0)
                    .build());
            
            Map<String, Object> exportData = indexExportService.exportIndex(
                    config, indexName, batchSize, includeSettings, includeMappings, includeAliases);

            byte[] jsonBytes = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(exportData);

            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("COMPLETED")
                    .message("Export completed: " + indexName)
                    .currentIndex(indexName)
                    .totalIndices(1)
                    .completedIndices(1)
                    .percentage(100)
                    .bytesProcessed(jsonBytes.length)
                    .build());

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setContentDispositionFormData("attachment", indexName + ".json");

            return ResponseEntity.ok().headers(headers).body(jsonBytes);
        } catch (Exception e) {
            log.error("Failed to export index: {}", indexName, e);
            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("FAILED")
                    .message("Export failed: " + e.getMessage())
                    .build());
            return ResponseEntity.internalServerError().build();
        }
    }

    // Export multiple indices as ZIP with progress
    @PostMapping("/clusters/{clusterId}/export")
    public ResponseEntity<byte[]> exportMultipleIndices(
            @PathVariable("clusterId") String clusterId,
            @RequestBody ExportRequest request) {

        Optional<ClusterConfigEntity> entity = clusterRepository.findById(clusterId);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String operationId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        try {
            ClusterConfig config = entity.get().toClusterConfig();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ZipOutputStream zos = new ZipOutputStream(baos);

            int totalIndices = request.indices.size();
            int completed = 0;

            for (String indexName : request.indices) {
                try {
                    broadcastProgress(ProgressInfo.builder()
                            .operationId(operationId)
                            .status("RUNNING")
                            .message("Exporting " + indexName)
                            .currentIndex(indexName)
                            .totalIndices(totalIndices)
                            .completedIndices(completed)
                            .percentage((double) completed / totalIndices * 100)
                            .build());

                    Map<String, Object> exportData = indexExportService.exportIndex(
                            config, indexName, request.batchSize,
                            request.includeSettings, request.includeMappings, request.includeAliases);

                    byte[] jsonBytes = objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsBytes(exportData);

                    ZipEntry zipEntry = new ZipEntry(indexName + ".json");
                    zos.putNextEntry(zipEntry);
                    zos.write(jsonBytes);
                    zos.closeEntry();

                    completed++;
                    log.info("Exported index {}/{}: {}", completed, totalIndices, indexName);
                } catch (Exception e) {
                    log.error("Failed to export index: {}", indexName, e);
                }
            }

            zos.close();

            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("COMPLETED")
                    .message("Export completed: " + completed + "/" + totalIndices + " indices")
                    .totalIndices(totalIndices)
                    .completedIndices(completed)
                    .percentage(100)
                    .bytesProcessed(baos.size())
                    .build());

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            String filename = "export_" + System.currentTimeMillis() + ".zip";
            headers.setContentDispositionFormData("attachment", filename);

            return ResponseEntity.ok().headers(headers).body(baos.toByteArray());
        } catch (Exception e) {
            log.error("Failed to export indices", e);
            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("FAILED")
                    .message("Export failed: " + e.getMessage())
                    .build());
            return ResponseEntity.internalServerError().build();
        }
    }

    // Import with progress
    @PostMapping("/clusters/{clusterId}/indices/import")
    public ResponseEntity<?> importIndex(
            @PathVariable("clusterId") String clusterId,
            @RequestParam("file") MultipartFile file) {

        Optional<ClusterConfigEntity> entity = clusterRepository.findById(clusterId);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String operationId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        try {
            ClusterConfig config = entity.get().toClusterConfig();
            String filename = file.getOriginalFilename();

            if (filename != null && filename.toLowerCase().endsWith(".zip")) {
                return importFromZip(config, file, operationId);
            }

            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("RUNNING")
                    .message("Importing from " + filename)
                    .percentage(0)
                    .build());

            @SuppressWarnings("unchecked")
            Map<String, Object> importData = objectMapper.readValue(file.getBytes(), Map.class);

            String indexName = (String) importData.get("name");
            if (indexName == null || indexName.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Invalid export file: missing index name"));
            }

            indexExportService.importIndex(config, importData);

            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("COMPLETED")
                    .message("Imported: " + indexName)
                    .currentIndex(indexName)
                    .percentage(100)
                    .build());

            return ResponseEntity.ok(Map.of(
                    "message", "Index imported successfully",
                    "indexName", indexName
            ));
        } catch (Exception e) {
            log.error("Failed to import index", e);
            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("FAILED")
                    .message("Import failed: " + e.getMessage())
                    .build());
            return ResponseEntity.badRequest().body(Map.of("error", "Import failed: " + e.getMessage()));
        }
    }

    @SuppressWarnings("unchecked")
    private ResponseEntity<?> importFromZip(ClusterConfig config, MultipartFile file, String operationId) {
        List<String> importedIndices = new ArrayList<>();
        List<String> failedIndices = new ArrayList<>();

        try (ZipInputStream zis = new ZipInputStream(file.getInputStream())) {
            // First pass: count entries
            List<byte[]> entries = new ArrayList<>();
            List<String> entryNames = new ArrayList<>();
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.getName().endsWith(".json")) {
                    entries.add(zis.readAllBytes());
                    entryNames.add(entry.getName());
                }
                zis.closeEntry();
            }

            int total = entries.size();
            int completed = 0;

            for (int i = 0; i < entries.size(); i++) {
                String name = entryNames.get(i);
                try {
                    broadcastProgress(ProgressInfo.builder()
                            .operationId(operationId)
                            .status("RUNNING")
                            .message("Importing " + name)
                            .totalIndices(total)
                            .completedIndices(completed)
                            .percentage((double) completed / total * 100)
                            .build());

                    Map<String, Object> importData = objectMapper.readValue(entries.get(i), Map.class);
                    String indexName = (String) importData.get("name");

                    if (indexName != null && !indexName.isEmpty()) {
                        indexExportService.importIndex(config, importData);
                        importedIndices.add(indexName);
                        completed++;
                    }
                } catch (Exception e) {
                    String failedName = name.replace(".json", "");
                    failedIndices.add(failedName);
                    log.error("Failed to import from ZIP: {}", name, e);
                }
            }

            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("COMPLETED")
                    .message("Import completed: " + importedIndices.size() + "/" + total)
                    .totalIndices(total)
                    .completedIndices(importedIndices.size())
                    .percentage(100)
                    .build());

        } catch (Exception e) {
            log.error("Failed to read ZIP file", e);
            broadcastProgress(ProgressInfo.builder()
                    .operationId(operationId)
                    .status("FAILED")
                    .message("Failed to read ZIP: " + e.getMessage())
                    .build());
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to read ZIP file: " + e.getMessage()));
        }

        return ResponseEntity.ok(Map.of(
                "message", "Import completed",
                "imported", importedIndices,
                "failed", failedIndices
        ));
    }

    @GetMapping("/operations")
    public ResponseEntity<List<TransferOperation>> getOperations() {
        return ResponseEntity.ok(new ArrayList<>(dataTransferService.getActiveOperations().values()));
    }

    @GetMapping("/operations/{id}/progress")
    public ResponseEntity<ProgressInfo> getOperationProgress(@PathVariable("id") String id) {
        ProgressInfo info = operationProgress.get(id);
        if (info == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(info);
    }

    @DeleteMapping("/operations/completed")
    public ResponseEntity<Void> clearCompleted() {
        dataTransferService.clearCompletedOperations();
        operationProgress.entrySet().removeIf(e -> 
            "COMPLETED".equals(e.getValue().getStatus()) || "FAILED".equals(e.getValue().getStatus()));
        return ResponseEntity.ok().build();
    }

    // Document viewer endpoint with pagination
    @GetMapping("/clusters/{clusterId}/indices/{indexName}/documents")
    public ResponseEntity<?> getDocuments(
            @PathVariable("clusterId") String clusterId,
            @PathVariable("indexName") String indexName,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size,
            @RequestParam(value = "q", required = false) String query) {
        
        Optional<ClusterConfigEntity> entity = clusterRepository.findById(clusterId);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        try {
            ClusterConfig config = entity.get().toClusterConfig();
            ClusterService service = serviceFactory.getService(config);
            
            Map<String, Object> result = service.searchDocuments(config, indexName, query, page, size);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            log.error("Failed to fetch documents from index {}", indexName, e);
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to fetch documents: " + e.getMessage()));
        }
    }

    // Get single document by ID
    @GetMapping("/clusters/{clusterId}/indices/{indexName}/documents/{docId}")
    public ResponseEntity<?> getDocument(
            @PathVariable("clusterId") String clusterId,
            @PathVariable("indexName") String indexName,
            @PathVariable("docId") String docId) {
        
        Optional<ClusterConfigEntity> entity = clusterRepository.findById(clusterId);
        if (entity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        try {
            ClusterConfig config = entity.get().toClusterConfig();
            ClusterService service = serviceFactory.getService(config);
            
            Map<String, Object> document = service.getDocument(config, indexName, docId);
            if (document == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(document);
        } catch (Exception e) {
            log.error("Failed to fetch document {} from index {}", docId, indexName, e);
            return ResponseEntity.badRequest().body(Map.of("error", "Failed to fetch document: " + e.getMessage()));
        }
    }

    public record TransferRequest(
            String sourceClusterId,
            String targetClusterId,
            List<String> indices,
            int batchSize,
            boolean includeSettings,
            boolean includeMappings,
            boolean includeAliases
    ) {}

    public record ExportRequest(
            List<String> indices,
            int batchSize,
            boolean includeSettings,
            boolean includeMappings,
            boolean includeAliases
    ) {}
}
