/**
 * Data Manager - Main JavaScript
 * Elasticsearch & OpenSearch Management Tool
 */

(function() {
    'use strict';

    // ============================================
    // State Management
    // ============================================
    var state = {
        clusters: [],
        sourceIndices: [],
        exportIndices: [],
        sourceSelected: new Set(),
        exportSelected: new Set(),
        activeOperations: {},
        eventSource: null,
        
        // Document viewer state
        currentClusterId: null,
        currentIndexName: null,
        documents: [],
        selectedDocument: null,
        pagination: {
            page: 0,
            size: 20,
            total: 0,
            totalPages: 0
        }
    };

    // ============================================
    // Initialization
    // ============================================
    document.addEventListener('DOMContentLoaded', function() {
        loadClusters();
        setupEventListeners();
        setupDropZone();
        connectSSE();
    });

    function setupEventListeners() {
        // Navigation
        var navItems = document.querySelectorAll('.nav-item');
        for (var i = 0; i < navItems.length; i++) {
            navItems[i].addEventListener('click', handleNavClick);
        }

        // Top bar buttons
        addClickListener('btnManageClusters', showManageClustersModal);
        addClickListener('btnAddCluster', showAddClusterModal);

        // Modal buttons
        addClickListener('closeAddModal', hideAddClusterModal);
        addClickListener('cancelAddCluster', hideAddClusterModal);
        addClickListener('saveClusterBtn', addCluster);
        addClickListener('closeManageModal', hideManageClustersModal);
        addClickListener('closeManageClusters', hideManageClustersModal);

        // Transfer tab
        addChangeListener('sourceCluster', function() { loadIndices('source'); });
        addClickListener('btnTestSource', function() { testConnection('source'); });
        addClickListener('btnTestTarget', function() { testConnection('target'); });
        addClickListener('btnSelectAllSource', function() { selectAllIndices('source', true); });
        addClickListener('btnClearSource', function() { selectAllIndices('source', false); });
        addChangeListener('sourceSelectAll', function() { selectAllIndices('source', this.checked); });
        addClickListener('btnStartTransfer', startTransfer);

        // Export tab
        addChangeListener('exportCluster', function() { loadIndices('export'); });
        addClickListener('btnRefreshExport', function() { loadIndices('export'); });
        addClickListener('btnSelectAllExport', function() { selectAllIndices('export', true); });
        addClickListener('btnClearExport', function() { selectAllIndices('export', false); });
        addChangeListener('exportSelectAll', function() { selectAllIndices('export', this.checked); });
        addClickListener('btnExportZip', exportSelectedAsZip);

        // Operations tab
        addClickListener('btnRefreshOps', refreshOperations);
        addClickListener('btnClearCompleted', clearCompleted);

        // Document viewer modal
        addClickListener('closeDocViewerModal', hideDocumentViewer);
        addClickListener('btnCloseDocViewer', hideDocumentViewer);
        addClickListener('btnPrevPage', function() { changePage(-1); });
        addClickListener('btnNextPage', function() { changePage(1); });
        addChangeListener('docPageSize', function() { 
            state.pagination.size = parseInt(this.value);
            state.pagination.page = 0;
            loadDocuments();
        });

        // Search in documents
        var searchInput = document.getElementById('docSearchInput');
        if (searchInput) {
            var searchTimeout;
            searchInput.addEventListener('input', function() {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(function() {
                    state.pagination.page = 0;
                    loadDocuments();
                }, 300);
            });
        }
    }

    function addClickListener(id, handler) {
        var el = document.getElementById(id);
        if (el) el.addEventListener('click', handler);
    }

    function addChangeListener(id, handler) {
        var el = document.getElementById(id);
        if (el) el.addEventListener('change', handler);
    }

    function handleNavClick() {
        var tab = this.getAttribute('data-tab');
        if (tab) showTab(tab);
    }

    // ============================================
    // Server-Sent Events (SSE)
    // ============================================
    function connectSSE() {
        if (state.eventSource) {
            state.eventSource.close();
        }

        state.eventSource = new EventSource('/api/progress');

        state.eventSource.addEventListener('progress', function(e) {
            var progress = JSON.parse(e.data);
            state.activeOperations[progress.operationId] = progress;
            renderActiveProgress();

            if (progress.status === 'COMPLETED') {
                showToast('Completed: ' + progress.message, 'success');
            } else if (progress.status === 'FAILED') {
                showToast('Failed: ' + progress.message, 'error');
            }
        });

        state.eventSource.onerror = function() {
            setTimeout(connectSSE, 5000);
        };
    }

    function renderActiveProgress() {
        var container = document.getElementById('activeProgress');
        if (!container) return;

        var running = [];
        for (var key in state.activeOperations) {
            if (state.activeOperations[key].status === 'RUNNING') {
                running.push(state.activeOperations[key]);
            }
        }

        if (running.length === 0) {
            container.innerHTML = '';
            return;
        }

        var html = running.map(function(p) {
            var pct = Math.round(p.percentage || 0);
            return '<div class="progress-container">' +
                '<div class="progress-header">' +
                '<div class="progress-title"><span class="status-dot running"></span> ' + escapeHtml(p.message || 'Processing...') + '</div>' +
                '<span class="progress-percentage">' + pct + '%</span></div>' +
                '<div class="progress-bar"><div class="progress-fill" style="width:' + pct + '%"></div></div>' +
                '<div class="progress-info"><span>' + (p.completedIndices || 0) + ' / ' + (p.totalIndices || 0) + ' indices</span>' +
                '<span>' + escapeHtml(p.currentIndex || '') + '</span></div></div>';
        }).join('');

        container.innerHTML = html;
    }

    // ============================================
    // API Functions
    // ============================================
    function loadClusters() {
        fetch('/api/clusters')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                state.clusters = data;
                updateClusterSelects();
                renderSidebarClusters();
            })
            .catch(function() {
                showToast('Failed to load clusters', 'error');
            });
    }

    function updateClusterSelects() {
        var opts = '<option value="">Choose a cluster...</option>';
        for (var i = 0; i < state.clusters.length; i++) {
            var c = state.clusters[i];
            opts += '<option value="' + c.id + '">' + escapeHtml(c.name) + ' (' + c.type + ')</option>';
        }

        var selectIds = ['sourceCluster', 'targetCluster', 'exportCluster', 'importCluster'];
        for (var j = 0; j < selectIds.length; j++) {
            var el = document.getElementById(selectIds[j]);
            if (el) el.innerHTML = opts;
        }
    }

    function renderSidebarClusters() {
        var container = document.getElementById('sidebarClusters');
        if (!container) return;

        if (state.clusters.length === 0) {
            container.innerHTML = '<div style="padding:12px;color:var(--text-muted);font-size:13px;">No clusters</div>';
            return;
        }

        var html = state.clusters.map(function(c) {
            var dotClass = c.type === 'ELASTICSEARCH' ? 'es' : 'os';
            return '<div class="cluster-item-nav" data-cluster-id="' + c.id + '">' +
                '<div class="cluster-dot ' + dotClass + '"></div>' +
                '<span>' + escapeHtml(c.name) + '</span></div>';
        }).join('');

        container.innerHTML = html;
    }

    function testConnection(type) {
        var selectId = type === 'source' ? 'sourceCluster' : (type === 'target' ? 'targetCluster' : type + 'Cluster');
        var clusterId = document.getElementById(selectId).value;

        if (!clusterId) {
            showToast('Select a cluster first', 'error');
            return;
        }

        fetch('/api/clusters/' + clusterId + '/test', { method: 'POST' })
            .then(function(r) {
                if (r.ok) return r.json();
                throw new Error('Connection failed');
            })
            .then(function(info) {
                showToast('Connected to ' + info.clusterName + ' v' + info.version, 'success');
            })
            .catch(function() {
                showToast('Connection failed', 'error');
            });
    }

    function loadIndices(type) {
        var selectId = type === 'source' ? 'sourceCluster' : 'exportCluster';
        var clusterId = document.getElementById(selectId).value;

        if (!clusterId) return;

        fetch('/api/clusters/' + clusterId + '/indices')
            .then(function(r) { return r.json(); })
            .then(function(indices) {
                if (type === 'source') {
                    state.sourceIndices = indices;
                    state.sourceSelected.clear();
                } else {
                    state.exportIndices = indices;
                    state.exportSelected.clear();
                }
                renderIndicesTable(type);
            })
            .catch(function() {
                showToast('Failed to load indices', 'error');
            });
    }

    function renderIndicesTable(type) {
        var indices = type === 'source' ? state.sourceIndices : state.exportIndices;
        var selected = type === 'source' ? state.sourceSelected : state.exportSelected;
        var tbody = document.getElementById(type + 'IndicesTable');

        if (!tbody) return;

        if (indices.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No indices found</td></tr>';
            return;
        }

        var selectId = type === 'source' ? 'sourceCluster' : 'exportCluster';
        var clusterId = document.getElementById(selectId).value;

        var html = indices.map(function(idx) {
            var health = (idx.health || 'unknown').toLowerCase();
            var checked = selected.has(idx.name) ? 'checked' : '';
            var badgeClass = health === 'green' ? 'badge-green' : (health === 'yellow' ? 'badge-yellow' : 'badge-red');

            var row = '<tr class="clickable" data-index="' + idx.name + '" data-cluster="' + clusterId + '">' +
                '<td class="checkbox-cell" onclick="event.stopPropagation()">' +
                '<input type="checkbox" class="checkbox index-checkbox" data-type="' + type + '" data-name="' + idx.name + '" ' + checked + '></td>' +
                '<td>' + escapeHtml(idx.name) + '</td>' +
                '<td>' + formatNumber(idx.documentCount || 0) + '</td>' +
                '<td>' + (idx.size || '0b') + '</td>' +
                '<td><span class="badge ' + badgeClass + '">' + health + '</span></td>';

            if (type === 'export') {
                row += '<td onclick="event.stopPropagation()">' +
                    '<button class="btn btn-sm btn-ghost export-single-btn" data-index="' + idx.name + '">📥 Export</button></td>';
            }

            row += '</tr>';
            return row;
        }).join('');

        tbody.innerHTML = html;

        // Add event listeners for checkboxes
        var checkboxes = tbody.querySelectorAll('.index-checkbox');
        for (var i = 0; i < checkboxes.length; i++) {
            checkboxes[i].addEventListener('change', handleIndexCheckboxChange);
        }

        // Add event listeners for rows (document viewer)
        var rows = tbody.querySelectorAll('tr.clickable');
        for (var j = 0; j < rows.length; j++) {
            rows[j].addEventListener('click', handleIndexRowClick);
        }

        // Add event listeners for export buttons
        var exportBtns = tbody.querySelectorAll('.export-single-btn');
        for (var k = 0; k < exportBtns.length; k++) {
            exportBtns[k].addEventListener('click', function(e) {
                e.stopPropagation();
                exportSingleIndex(this.getAttribute('data-index'));
            });
        }

        updateSelectedCount(type);
    }

    function handleIndexCheckboxChange() {
        var type = this.getAttribute('data-type');
        var name = this.getAttribute('data-name');
        toggleIndex(type, name);
    }

    function handleIndexRowClick() {
        var indexName = this.getAttribute('data-index');
        var clusterId = this.getAttribute('data-cluster');
        openDocumentViewer(clusterId, indexName);
    }

    function toggleIndex(type, name) {
        var selected = type === 'source' ? state.sourceSelected : state.exportSelected;
        if (selected.has(name)) {
            selected.delete(name);
        } else {
            selected.add(name);
        }
        updateSelectedCount(type);
    }

    function selectAllIndices(type, select) {
        var indices = type === 'source' ? state.sourceIndices : state.exportIndices;
        var selected = type === 'source' ? state.sourceSelected : state.exportSelected;

        selected.clear();
        if (select) {
            for (var i = 0; i < indices.length; i++) {
                selected.add(indices[i].name);
            }
        }
        renderIndicesTable(type);
    }

    function updateSelectedCount(type) {
        var selected = type === 'source' ? state.sourceSelected : state.exportSelected;
        var el = document.getElementById(type + 'SelectedCount');
        if (el) el.textContent = selected.size;
    }

    // ============================================
    // Transfer Functions
    // ============================================
    function startTransfer() {
        var sourceId = document.getElementById('sourceCluster').value;
        var targetId = document.getElementById('targetCluster').value;

        if (!sourceId || !targetId) {
            showToast('Select source and target clusters', 'error');
            return;
        }

        if (state.sourceSelected.size === 0) {
            showToast('Select at least one index', 'error');
            return;
        }

        var request = {
            sourceClusterId: sourceId,
            targetClusterId: targetId,
            indices: Array.from(state.sourceSelected),
            batchSize: parseInt(document.getElementById('transferBatchSize').value) || 1000,
            includeSettings: document.getElementById('transferIncludeSettings').checked,
            includeMappings: document.getElementById('transferIncludeMappings').checked,
            includeAliases: document.getElementById('transferIncludeAliases').checked
        };

        fetch('/api/transfer', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        })
        .then(function(r) { return r.json(); })
        .then(function(result) {
            showToast('Transfer started: ' + result.operationId, 'info');
        })
        .catch(function() {
            showToast('Failed to start transfer', 'error');
        });
    }

    // ============================================
    // Export Functions
    // ============================================
    function exportSingleIndex(indexName) {
        var clusterId = document.getElementById('exportCluster').value;
        if (!clusterId) {
            showToast('Select a cluster', 'error');
            return;
        }

        var params = 'includeSettings=' + document.getElementById('exportIncludeSettings').checked +
            '&includeMappings=' + document.getElementById('exportIncludeMappings').checked +
            '&includeAliases=' + document.getElementById('exportIncludeAliases').checked +
            '&batchSize=' + document.getElementById('exportBatchSize').value;

        showToast('Exporting ' + indexName + '...', 'info');

        fetch('/api/clusters/' + clusterId + '/indices/' + indexName + '/export?' + params)
            .then(function(r) {
                if (r.ok) return r.blob();
                throw new Error('Export failed');
            })
            .then(function(blob) {
                downloadBlob(blob, indexName + '.json');
                showToast('Exported: ' + indexName, 'success');
            })
            .catch(function() {
                showToast('Export failed', 'error');
            });
    }

    function exportSelectedAsZip() {
        var clusterId = document.getElementById('exportCluster').value;
        if (!clusterId) {
            showToast('Select a cluster', 'error');
            return;
        }

        if (state.exportSelected.size === 0) {
            showToast('Select at least one index', 'error');
            return;
        }

        var request = {
            indices: Array.from(state.exportSelected),
            batchSize: parseInt(document.getElementById('exportBatchSize').value) || 1000,
            includeSettings: document.getElementById('exportIncludeSettings').checked,
            includeMappings: document.getElementById('exportIncludeMappings').checked,
            includeAliases: document.getElementById('exportIncludeAliases').checked
        };

        showToast('Exporting ' + request.indices.length + ' indices...', 'info');

        fetch('/api/clusters/' + clusterId + '/export', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request)
        })
        .then(function(r) {
            if (r.ok) return r.blob();
            throw new Error('Export failed');
        })
        .then(function(blob) {
            downloadBlob(blob, 'export_' + Date.now() + '.zip');
            showToast('Export completed', 'success');
        })
        .catch(function() {
            showToast('Export failed', 'error');
        });
    }

    // ============================================
    // Import Functions
    // ============================================
    function setupDropZone() {
        var dropZone = document.getElementById('dropZone');
        var fileInput = document.getElementById('importFileInput');

        if (!dropZone || !fileInput) return;

        dropZone.addEventListener('click', function() {
            fileInput.click();
        });

        dropZone.addEventListener('dragover', function(e) {
            e.preventDefault();
            dropZone.classList.add('drag-over');
        });

        dropZone.addEventListener('dragleave', function() {
            dropZone.classList.remove('drag-over');
        });

        dropZone.addEventListener('drop', function(e) {
            e.preventDefault();
            dropZone.classList.remove('drag-over');
            handleFiles(e.dataTransfer.files);
        });

        fileInput.addEventListener('change', function(e) {
            handleFiles(e.target.files);
            e.target.value = '';
        });
    }

    function handleFiles(files) {
        var clusterId = document.getElementById('importCluster').value;
        if (!clusterId) {
            showToast('Select a target cluster', 'error');
            return;
        }

        var statusDiv = document.getElementById('importStatus');
        if (statusDiv) statusDiv.innerHTML = '';

        for (var i = 0; i < files.length; i++) {
            importFile(clusterId, files[i], statusDiv);
        }
    }

    function importFile(clusterId, file, statusDiv) {
        var formData = new FormData();
        formData.append('file', file);

        showToast('Importing ' + file.name + '...', 'info');

        fetch('/api/clusters/' + clusterId + '/indices/import', {
            method: 'POST',
            body: formData
        })
        .then(function(r) {
            return r.json().then(function(data) {
                return { ok: r.ok, data: data };
            });
        })
        .then(function(result) {
            if (result.ok) {
                var msg = result.data.imported ? result.data.imported.join(', ') : result.data.indexName;
                if (statusDiv) {
                    statusDiv.innerHTML += '<div class="operation-item">' +
                        '<div class="operation-icon import">✅</div>' +
                        '<div class="operation-details">' +
                        '<div class="operation-name">' + escapeHtml(file.name) + '</div>' +
                        '<div class="operation-meta">Imported: ' + escapeHtml(msg) + '</div>' +
                        '</div></div>';
                }
                showToast('Import successful', 'success');
            } else {
                if (statusDiv) {
                    statusDiv.innerHTML += '<div class="operation-item">' +
                        '<div class="operation-icon" style="background:var(--accent-red-light);color:var(--accent-red);">❌</div>' +
                        '<div class="operation-details">' +
                        '<div class="operation-name">' + escapeHtml(file.name) + '</div>' +
                        '<div class="operation-meta">' + escapeHtml(result.data.error) + '</div>' +
                        '</div></div>';
                }
                showToast('Import failed', 'error');
            }
        })
        .catch(function() {
            showToast('Import failed', 'error');
        });
    }

    // ============================================
    // Operations Functions
    // ============================================
    function refreshOperations() {
        fetch('/api/operations')
            .then(function(r) { return r.json(); })
            .then(function(ops) {
                renderOperationsList(ops);
            })
            .catch(function() {
                showToast('Failed to load operations', 'error');
            });
    }

    function renderOperationsList(ops) {
        var container = document.getElementById('operationsList');
        if (!container) return;

        var allOps = ops.slice();
        for (var key in state.activeOperations) {
            allOps.push(state.activeOperations[key]);
        }

        if (allOps.length === 0) {
            container.innerHTML = '<div class="empty-state">' +
                '<div class="empty-state-icon">📭</div>' +
                '<div class="empty-state-title">No operations</div>' +
                '<div class="empty-state-text">Operations will appear here when you start a transfer, export, or import.</div>' +
                '</div>';
            return;
        }

        var html = allOps.map(function(op) {
            var progress = state.activeOperations[op.operationId] || op;
            var pct = progress.percentage || op.progress || 0;
            var status = progress.status || op.status || 'PENDING';

            return '<div class="operation-item">' +
                '<div class="operation-icon transfer">🔄</div>' +
                '<div class="operation-details">' +
                '<div class="operation-name">' + escapeHtml(progress.message || op.operationId || 'Operation') + '</div>' +
                '<div class="operation-meta">' + status + '</div></div>' +
                '<div class="operation-progress">' +
                '<div class="mini-progress"><div class="mini-progress-fill" style="width:' + pct + '%"></div></div>' +
                '<div class="mini-progress-text">' + Math.round(pct) + '%</div></div></div>';
        }).join('');

        container.innerHTML = html;
    }

    function clearCompleted() {
        fetch('/api/operations/completed', { method: 'DELETE' })
            .then(function() {
                state.activeOperations = {};
                renderActiveProgress();
                refreshOperations();
            });
    }

    // ============================================
    // Document Viewer Functions
    // ============================================
    function openDocumentViewer(clusterId, indexName) {
        state.currentClusterId = clusterId;
        state.currentIndexName = indexName;
        state.pagination.page = 0;
        state.selectedDocument = null;
        state.documents = [];

        var modal = document.getElementById('documentViewerModal');
        var titleEl = document.getElementById('docViewerTitle');
        
        if (titleEl) {
            titleEl.textContent = 'Index: ' + indexName;
        }

        if (modal) {
            modal.classList.add('show');
        }

        loadDocuments();
    }

    function hideDocumentViewer() {
        var modal = document.getElementById('documentViewerModal');
        if (modal) {
            modal.classList.remove('show');
        }
        state.currentClusterId = null;
        state.currentIndexName = null;
        state.documents = [];
        state.selectedDocument = null;
    }

    function loadDocuments() {
        if (!state.currentClusterId || !state.currentIndexName) return;

        var searchInput = document.getElementById('docSearchInput');
        var searchQuery = searchInput ? searchInput.value.trim() : '';

        var url = '/api/clusters/' + state.currentClusterId + '/indices/' + state.currentIndexName + '/documents?' +
            'page=' + state.pagination.page +
            '&size=' + state.pagination.size;

        if (searchQuery) {
            url += '&q=' + encodeURIComponent(searchQuery);
        }

        var listContent = document.getElementById('documentListContent');
        if (listContent) {
            listContent.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⏳</div><p>Loading...</p></div>';
        }

        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                state.documents = data.documents || [];
                state.pagination.total = data.total || 0;
                state.pagination.totalPages = Math.ceil(state.pagination.total / state.pagination.size);
                renderDocumentList();
                updatePagination();

                // Select first document if available
                if (state.documents.length > 0 && !state.selectedDocument) {
                    selectDocument(0);
                }
            })
            .catch(function(e) {
                showToast('Failed to load documents', 'error');
                if (listContent) {
                    listContent.innerHTML = '<div class="empty-state"><div class="empty-state-icon">❌</div><p>Failed to load documents</p></div>';
                }
            });
    }

    function renderDocumentList() {
        var container = document.getElementById('documentListContent');
        if (!container) return;

        if (state.documents.length === 0) {
            container.innerHTML = '<div class="empty-state">' +
                '<div class="empty-state-icon">📄</div>' +
                '<div class="empty-state-title">No documents</div>' +
                '<div class="empty-state-text">This index has no documents or no matches found.</div>' +
                '</div>';
            return;
        }

        var html = state.documents.map(function(doc, index) {
            var isActive = state.selectedDocument && state.selectedDocument._id === doc._id;
            var preview = getDocumentPreview(doc._source);

            return '<div class="document-item' + (isActive ? ' active' : '') + '" data-index="' + index + '">' +
                '<div class="document-id">' + escapeHtml(doc._id) + '</div>' +
                '<div class="document-preview">' + escapeHtml(preview) + '</div>' +
                '</div>';
        }).join('');

        container.innerHTML = html;

        // Add click listeners
        var items = container.querySelectorAll('.document-item');
        for (var i = 0; i < items.length; i++) {
            items[i].addEventListener('click', function() {
                var idx = parseInt(this.getAttribute('data-index'));
                selectDocument(idx);
            });
        }
    }

    function getDocumentPreview(source) {
        if (!source) return 'Empty document';
        var keys = Object.keys(source).slice(0, 3);
        var preview = keys.map(function(k) {
            var v = source[k];
            if (typeof v === 'object') v = '{...}';
            if (typeof v === 'string' && v.length > 30) v = v.substring(0, 30) + '...';
            return k + ': ' + v;
        }).join(', ');
        return preview || 'Empty document';
    }

    function selectDocument(index) {
        if (index < 0 || index >= state.documents.length) return;

        state.selectedDocument = state.documents[index];
        renderDocumentList();
        renderDocumentDetail();
    }

    function renderDocumentDetail() {
        var container = document.getElementById('jsonViewer');
        if (!container) return;

        if (!state.selectedDocument) {
            container.innerHTML = '<div class="empty-state">' +
                '<div class="empty-state-icon">📄</div>' +
                '<p>Select a document to view</p></div>';
            return;
        }

        var json = {
            _id: state.selectedDocument._id,
            _index: state.selectedDocument._index,
            _source: state.selectedDocument._source
        };

        container.innerHTML = formatJson(json, 0);
    }

    function formatJson(obj, indent) {
        if (obj === null) return '<span class="json-null">null</span>';
        if (obj === undefined) return '<span class="json-null">undefined</span>';

        var type = typeof obj;

        if (type === 'boolean') {
            return '<span class="json-boolean">' + obj + '</span>';
        }

        if (type === 'number') {
            return '<span class="json-number">' + obj + '</span>';
        }

        if (type === 'string') {
            return '<span class="json-string">"' + escapeHtml(obj) + '"</span>';
        }

        if (Array.isArray(obj)) {
            if (obj.length === 0) return '<span class="json-bracket">[]</span>';

            var arrHtml = '<span class="json-bracket">[</span>\n';
            for (var i = 0; i < obj.length; i++) {
                arrHtml += getIndent(indent + 1) + formatJson(obj[i], indent + 1);
                if (i < obj.length - 1) arrHtml += ',';
                arrHtml += '\n';
            }
            arrHtml += getIndent(indent) + '<span class="json-bracket">]</span>';
            return arrHtml;
        }

        if (type === 'object') {
            var keys = Object.keys(obj);
            if (keys.length === 0) return '<span class="json-bracket">{}</span>';

            var objHtml = '<span class="json-bracket">{</span>\n';
            for (var j = 0; j < keys.length; j++) {
                var key = keys[j];
                objHtml += getIndent(indent + 1) + '<span class="json-key">"' + escapeHtml(key) + '"</span>: ';
                objHtml += formatJson(obj[key], indent + 1);
                if (j < keys.length - 1) objHtml += ',';
                objHtml += '\n';
            }
            objHtml += getIndent(indent) + '<span class="json-bracket">}</span>';
            return objHtml;
        }

        return String(obj);
    }

    function getIndent(level) {
        var indent = '';
        for (var i = 0; i < level; i++) {
            indent += '  ';
        }
        return indent;
    }

    function updatePagination() {
        var pageInfo = document.getElementById('pageInfo');
        var prevBtn = document.getElementById('btnPrevPage');
        var nextBtn = document.getElementById('btnNextPage');
        var totalInfo = document.getElementById('docTotalCount');

        if (pageInfo) {
            pageInfo.textContent = (state.pagination.page + 1) + ' / ' + Math.max(1, state.pagination.totalPages);
        }

        if (totalInfo) {
            totalInfo.textContent = formatNumber(state.pagination.total) + ' documents';
        }

        if (prevBtn) {
            prevBtn.disabled = state.pagination.page === 0;
        }

        if (nextBtn) {
            nextBtn.disabled = state.pagination.page >= state.pagination.totalPages - 1;
        }
    }

    function changePage(delta) {
        var newPage = state.pagination.page + delta;
        if (newPage >= 0 && newPage < state.pagination.totalPages) {
            state.pagination.page = newPage;
            state.selectedDocument = null;
            loadDocuments();
        }
    }

    // ============================================
    // Modal Functions
    // ============================================
    function showAddClusterModal() {
        document.getElementById('addClusterError').style.display = 'none';
        document.getElementById('clusterName').value = '';
        document.getElementById('clusterHost').value = 'localhost';
        document.getElementById('clusterPort').value = '9200';
        document.getElementById('clusterUser').value = '';
        document.getElementById('clusterPass').value = '';
        document.getElementById('clusterSSL').checked = false;
        document.getElementById('addClusterModal').classList.add('show');
    }

    function hideAddClusterModal() {
        document.getElementById('addClusterModal').classList.remove('show');
    }

    function showManageClustersModal() {
        renderClustersList();
        document.getElementById('manageClustersModal').classList.add('show');
    }

    function hideManageClustersModal() {
        document.getElementById('manageClustersModal').classList.remove('show');
    }

    function renderClustersList() {
        var container = document.getElementById('clustersList');
        if (!container) return;

        if (state.clusters.length === 0) {
            container.innerHTML = '<div class="empty-state">' +
                '<div class="empty-state-icon">🔌</div>' +
                '<div class="empty-state-title">No clusters</div>' +
                '<div class="empty-state-text">Add a cluster to get started.</div>' +
                '</div>';
            return;
        }

        var html = state.clusters.map(function(c) {
            var icon = c.type === 'ELASTICSEARCH' ? '🔶' : '🔷';
            return '<div class="operation-item">' +
                '<div class="operation-icon ' + (c.type === 'ELASTICSEARCH' ? 'export' : 'import') + '">' + icon + '</div>' +
                '<div class="operation-details">' +
                '<div class="operation-name">' + escapeHtml(c.name) + '</div>' +
                '<div class="operation-meta">' + escapeHtml(c.host) + ':' + c.port + ' • ' + c.type + '</div></div>' +
                '<button class="btn btn-sm btn-danger delete-cluster-btn" data-id="' + c.id + '" data-name="' + escapeHtml(c.name) + '">Delete</button></div>';
        }).join('');

        container.innerHTML = html;

        // Add delete listeners
        var btns = container.querySelectorAll('.delete-cluster-btn');
        for (var i = 0; i < btns.length; i++) {
            btns[i].addEventListener('click', function() {
                deleteCluster(this.getAttribute('data-id'), this.getAttribute('data-name'));
            });
        }
    }

    function addCluster() {
        var errorDiv = document.getElementById('addClusterError');
        var btn = document.getElementById('saveClusterBtn');

        var config = {
            name: document.getElementById('clusterName').value.trim(),
            type: document.getElementById('clusterType').value,
            host: document.getElementById('clusterHost').value.trim(),
            port: parseInt(document.getElementById('clusterPort').value),
            username: document.getElementById('clusterUser').value,
            password: document.getElementById('clusterPass').value,
            useSSL: document.getElementById('clusterSSL').checked
        };

        if (!config.name || !config.host) {
            errorDiv.textContent = 'Name and Host are required';
            errorDiv.style.display = 'block';
            return;
        }

        btn.disabled = true;
        btn.textContent = 'Testing...';
        errorDiv.style.display = 'none';

        fetch('/api/clusters', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        })
        .then(function(res) {
            if (res.ok) return res.json();
            return res.json().then(function(e) {
                throw new Error(e.error || 'Failed');
            });
        })
        .then(function(cluster) {
            state.clusters.push(cluster);
            updateClusterSelects();
            renderSidebarClusters();
            hideAddClusterModal();
            showToast('Cluster added: ' + cluster.name, 'success');
        })
        .catch(function(e) {
            errorDiv.textContent = e.message;
            errorDiv.style.display = 'block';
        })
        .finally(function() {
            btn.disabled = false;
            btn.textContent = 'Save & Test';
        });
    }

    function deleteCluster(id, name) {
        if (!confirm('Delete cluster "' + name + '"?')) return;

        fetch('/api/clusters/' + id, { method: 'DELETE' })
            .then(function() {
                state.clusters = state.clusters.filter(function(c) {
                    return c.id !== id;
                });
                updateClusterSelects();
                renderSidebarClusters();
                renderClustersList();
                showToast('Cluster deleted', 'success');
            })
            .catch(function() {
                showToast('Failed to delete', 'error');
            });
    }

    // ============================================
    // Tab Navigation
    // ============================================
    function showTab(tabName) {
        var navItems = document.querySelectorAll('.nav-item');
        for (var i = 0; i < navItems.length; i++) {
            navItems[i].classList.remove('active');
            if (navItems[i].getAttribute('data-tab') === tabName) {
                navItems[i].classList.add('active');
            }
        }

        var panels = document.querySelectorAll('.tab-panel');
        for (var j = 0; j < panels.length; j++) {
            panels[j].classList.remove('active');
        }

        var activePanel = document.getElementById(tabName + 'Tab');
        if (activePanel) activePanel.classList.add('active');

        var titles = {
            transfer: 'Data Transfer',
            export: 'Export Indices',
            import: 'Import Indices',
            operations: 'Operations'
        };

        var titleEl = document.getElementById('pageTitle');
        if (titleEl) titleEl.textContent = titles[tabName] || 'Data Manager';

        if (tabName === 'operations') {
            refreshOperations();
        }
    }

    // ============================================
    // Utility Functions
    // ============================================
    function showToast(message, type) {
        var container = document.getElementById('toastContainer');
        if (!container) return;

        var icons = {
            success: '✓',
            error: '✕',
            info: 'ℹ',
            warning: '⚠'
        };

        var toast = document.createElement('div');
        toast.className = 'toast ' + type;
        toast.innerHTML = '<span class="toast-icon">' + icons[type] + '</span>' +
            '<span class="toast-message">' + escapeHtml(message) + '</span>';

        container.appendChild(toast);

        setTimeout(function() {
            toast.style.opacity = '0';
            toast.style.transform = 'translateX(100%)';
            setTimeout(function() {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
            }, 300);
        }, 4000);
    }

    function downloadBlob(blob, filename) {
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    function escapeHtml(text) {
        if (text === null || text === undefined) return '';
        var div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function formatNumber(num) {
        return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
    }

    // Make deleteCluster available globally for inline onclick
    window.deleteCluster = deleteCluster;

})();
