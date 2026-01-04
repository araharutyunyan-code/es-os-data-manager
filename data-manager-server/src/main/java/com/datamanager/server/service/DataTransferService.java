package com.datamanager.server.service;

import com.datamanager.common.exception.DataTransferException;
import com.datamanager.common.model.ClusterConfig;
import com.datamanager.common.model.ProgressInfo;
import com.datamanager.common.model.TransferOperation;
import com.datamanager.common.service.ClusterService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
@Service
public class DataTransferService {

    private final ClusterServiceFactory serviceFactory;
    private final Map<String, TransferOperation> activeOperations = new ConcurrentHashMap<>();

    @Autowired
    public DataTransferService(ClusterServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    @Async("taskExecutor")
    public void executeTransfer(TransferOperation operation, Consumer<ProgressInfo> progressCallback) {
        String operationId = operation.getOperationId();
        if (operationId == null) {
            operationId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
            operation.setOperationId(operationId);
        }
        
        operation.setStatus(TransferOperation.TransferStatus.RUNNING);
        operation.setStartTime(LocalDateTime.now());
        activeOperations.put(operationId, operation);

        final String opId = operationId;

        try {
            sendProgress(progressCallback, ProgressInfo.builder()
                    .operationId(opId)
                    .status("RUNNING")
                    .message("Starting transfer...")
                    .percentage(0)
                    .totalIndices(operation.getIndices().size())
                    .completedIndices(0)
                    .build());

            switch (operation.getTransferType()) {
                case CLUSTER_TO_CLUSTER -> executeClusterToCluster(operation, progressCallback);
                case CLUSTER_TO_FILE -> executeClusterToFile(operation, progressCallback);
                case FILE_TO_CLUSTER -> executeFileToCluster(operation, progressCallback);
            }

            operation.setStatus(TransferOperation.TransferStatus.COMPLETED);
            operation.setEndTime(LocalDateTime.now());
            
            sendProgress(progressCallback, ProgressInfo.builder()
                    .operationId(opId)
                    .status("COMPLETED")
                    .message("Transfer completed successfully")
                    .percentage(100)
                    .totalIndices(operation.getIndices().size())
                    .completedIndices(operation.getIndices().size())
                    .processedDocuments(operation.getProcessedDocuments())
                    .build());
            
            log.info("Transfer operation {} completed successfully", opId);

        } catch (Exception e) {
            operation.setStatus(TransferOperation.TransferStatus.FAILED);
            operation.setErrorMessage(e.getMessage());
            operation.setEndTime(LocalDateTime.now());
            
            sendProgress(progressCallback, ProgressInfo.builder()
                    .operationId(opId)
                    .status("FAILED")
                    .message("Transfer failed: " + e.getMessage())
                    .build());
            
            log.error("Transfer operation {} failed", opId, e);
        }
    }

    private void executeClusterToCluster(TransferOperation operation, Consumer<ProgressInfo> progressCallback) 
            throws DataTransferException {
        ClusterConfig source = operation.getSourceCluster();
        ClusterConfig target = operation.getTargetCluster();

        ClusterService sourceService = serviceFactory.getService(source);
        ClusterService targetService = serviceFactory.getService(target);

        List<String> indices = operation.getIndices();
        int batchSize = operation.getBatchSize();
        boolean includeSettings = operation.isIncludeSettings();
        boolean includeMappings = operation.isIncludeMappings();
        boolean includeAliases = operation.isIncludeAliases();

        int totalIndices = indices.size();
        int completedIndices = 0;
        long totalDocuments = 0;

        for (String indexName : indices) {
            log.info("Transferring index: {}", indexName);

            sendProgress(progressCallback, ProgressInfo.builder()
                    .operationId(operation.getOperationId())
                    .status("RUNNING")
                    .message("Exporting " + indexName + " from source...")
                    .currentIndex(indexName)
                    .totalIndices(totalIndices)
                    .completedIndices(completedIndices)
                    .percentage((double) completedIndices / totalIndices * 100)
                    .build());

            // Export from source
            Map<String, Object> indexData = sourceService.exportIndexData(
                    source, indexName, batchSize, includeSettings, includeMappings, includeAliases);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> docs = (List<Map<String, Object>>) indexData.get("documents");
            int docCount = docs != null ? docs.size() : 0;
            totalDocuments += docCount;

            sendProgress(progressCallback, ProgressInfo.builder()
                    .operationId(operation.getOperationId())
                    .status("RUNNING")
                    .message("Importing " + indexName + " to target (" + docCount + " docs)...")
                    .currentIndex(indexName)
                    .totalIndices(totalIndices)
                    .completedIndices(completedIndices)
                    .percentage((double) completedIndices / totalIndices * 100 + (50.0 / totalIndices))
                    .build());

            // Import to target
            targetService.importIndexData(target, indexData);

            completedIndices++;
            operation.setProcessedDocuments(totalDocuments);
            
            log.info("Transferred index: {} ({} documents)", indexName, docCount);
        }
    }

    private void executeClusterToFile(TransferOperation operation, Consumer<ProgressInfo> progressCallback) 
            throws DataTransferException {
        ClusterService service = serviceFactory.getService(operation.getSourceCluster());

        service.exportToFile(
                operation.getSourceCluster(),
                operation.getIndices(),
                operation.getExportFilePath(),
                operation.getBatchSize(),
                operation.isIncludeSettings(),
                operation.isIncludeMappings(),
                operation.isIncludeAliases()
        );
    }

    private void executeFileToCluster(TransferOperation operation, Consumer<ProgressInfo> progressCallback) 
            throws DataTransferException {
        ClusterService service = serviceFactory.getService(operation.getTargetCluster());

        service.importFromFile(
                operation.getTargetCluster(),
                operation.getExportFilePath()
        );
    }

    private void sendProgress(Consumer<ProgressInfo> callback, ProgressInfo progress) {
        if (callback != null) {
            try {
                callback.accept(progress);
            } catch (Exception e) {
                log.warn("Failed to send progress update", e);
            }
        }
    }

    public TransferOperation getOperation(String operationId) {
        return activeOperations.get(operationId);
    }

    public void cancelOperation(String operationId) {
        TransferOperation operation = activeOperations.get(operationId);
        if (operation != null && operation.getStatus() == TransferOperation.TransferStatus.RUNNING) {
            operation.setStatus(TransferOperation.TransferStatus.CANCELLED);
            operation.setEndTime(LocalDateTime.now());
        }
    }

    public Map<String, TransferOperation> getActiveOperations() {
        return new ConcurrentHashMap<>(activeOperations);
    }

    public void clearCompletedOperations() {
        activeOperations.entrySet().removeIf(entry -> {
            TransferOperation.TransferStatus status = entry.getValue().getStatus();
            return status == TransferOperation.TransferStatus.COMPLETED
                    || status == TransferOperation.TransferStatus.FAILED
                    || status == TransferOperation.TransferStatus.CANCELLED;
        });
    }
}
