import { api } from '../services/apiService.js';
import { ui } from '../utils/ui.js';

const syncManager = {
    container: null,
    connId: null,
    elements: {},
    sseSource: null,
    statusInterval: null,

    init(containerElement, connId) {
        this.container = containerElement;
        this.connId = connId;
        this.render();
        this.addEventListeners();
        this.loadMappingConfig();
        this.setupSse();
        this.startStatusPolling();
    },

    destroy() {
        this.closeSse();
        this.stopStatusPolling();
        this.container = null;
        this.connId = null;
        this.elements = {};
    },

    render() {
        this.container.innerHTML = this.getHtmlTemplate();
        this.elements = {
            triggerSyncBtn: document.getElementById('triggerSyncBtn'),
            saveMappingBtn: document.getElementById('saveMappingBtn'),
            mappingEditor: document.getElementById('mappingConfigEditor'),
            statusDetails: document.getElementById('syncStatusDetails'),
            liveLogContent: document.getElementById('liveLogContent'),
            historyContent: document.getElementById('historyContent'),
            liveLogTab: this.container.querySelector('[data-tab="live"]'),
            historyTab: this.container.querySelector('[data-tab="history"]'),
            liveLogWrapper: this.container.querySelector('#liveLogWrapper'),
            historyWrapper: this.container.querySelector('#historyWrapper'),
        };
    },

    getHtmlTemplate() {
        return `
            <div class="sync-grid">
                <div class="sync-section">
                    <h3>任务控制</h3>
                    <div class="btn-group">
                        <button id="triggerSyncBtn" class="btn btn-primary">开始同步</button>
                    </div>
                    <div id="syncStatusDetails">
                        <p><strong>状态:</strong> <span id="status-text">未知</span></p>
                        <p><strong>上次同步时间:</strong> <span id="last-sync-time">N/A</span></p>
                        <p><strong>耗时:</strong> <span id="last-sync-duration">N/A</span></p>
                    </div>
                </div>
                <div class="sync-section">
                    <h3>映射配置 (YAML)</h3>
                    <textarea id="mappingConfigEditor" placeholder="正在加载映射配置..."></textarea>
                    <button id="saveMappingBtn" class="btn btn-success">保存映射配置</button>
                </div>
                <div class="sync-section sync-console">
                    <h3>同步监控控制台</h3>
                    <div class="console-tabs">
                        <button class="console-tab active" data-tab="live">实时日志</button>
                        <button class="console-tab" data-tab="history">历史记录</button>
                    </div>
                    <div class="console-content-wrapper">
                        <div class="console-content active" id="liveLogWrapper">
                            <pre id="liveLogContent"></pre>
                        </div>
                        <div class="console-content" id="historyWrapper">
                            <div id="historyContent"></div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    },

    addEventListeners() {
        this.elements.triggerSyncBtn.addEventListener('click', () => this.triggerSync());
        this.elements.saveMappingBtn.addEventListener('click', () => this.saveMappingConfig());

        this.elements.liveLogTab.addEventListener('click', () => this.switchConsoleTab('live'));
        this.elements.historyTab.addEventListener('click', () => this.switchConsoleTab('history'));
    },

    switchConsoleTab(tabName) {
        if (tabName === 'live') {
            this.elements.liveLogTab.classList.add('active');
            this.elements.historyTab.classList.remove('active');
            this.elements.liveLogWrapper.classList.add('active');
            this.elements.historyWrapper.classList.remove('active');
        } else {
            this.elements.liveLogTab.classList.remove('active');
            this.elements.historyTab.classList.add('active');
            this.elements.liveLogWrapper.classList.remove('active');
            this.elements.historyWrapper.classList.add('active');
            this.updateStatus(); // Refresh history when switching to it
        }
    },

    async loadMappingConfig() {
        try {
            const config = await api.getMappingConfig(this.connId);
            if (this.elements.mappingEditor) {
                this.elements.mappingEditor.value = config;
            }
        } catch (error) {
            ui.showToast('加载映射配置失败', 'error');
            console.error(error);
            if (this.elements.mappingEditor) {
                this.elements.mappingEditor.value = "# 加载配置失败，请检查后端服务。";
            }
        }
    },

    async saveMappingConfig() {
        const config = this.elements.mappingEditor.value;
        try {
            await api.saveSyncMapping(this.connId, config);
            ui.showToast('映射配置已保存', 'success');
        } catch (error) {
            ui.showToast('保存映射配置失败', 'error');
            console.error(error);
        }
    },

    async triggerSync() {
        try {
            this.elements.triggerSyncBtn.disabled = true;
            this.elements.triggerSyncBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 正在触发...';
            this.switchConsoleTab('live');
            this.elements.liveLogContent.textContent = '--- 触发同步 ---\n';
            const result = await api.triggerSync(this.connId);
            ui.showToast(result.message, 'success');
        } catch (error) {
            ui.showToast('触发同步失败', 'error');
            console.error(error);
        }
    },

    startStatusPolling() {
        this.updateStatus(); // Initial call
        this.statusInterval = setInterval(() => this.updateStatus(), 5000);
    },

    stopStatusPolling() {
        if (this.statusInterval) {
            clearInterval(this.statusInterval);
            this.statusInterval = null;
        }
    },

    async updateStatus() {
        if (!this.connId || !document.getElementById('status-text')) return;
        try {
            const status = await api.getSyncStatus(this.connId);

            document.getElementById('status-text').textContent = status.status || '未知';

            if (this.elements.triggerSyncBtn) {
                if (status.status === 'IN_PROGRESS') {
                    this.elements.triggerSyncBtn.disabled = true;
                    this.elements.triggerSyncBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 正在同步...';
                } else {
                    this.elements.triggerSyncBtn.disabled = false;
                    this.elements.triggerSyncBtn.innerHTML = '开始同步';
                }
            }

            document.getElementById('last-sync-time').textContent = status.lastSyncTime ? new Date(status.lastSyncTime).toLocaleString() : 'N/A';
            document.getElementById('last-sync-duration').textContent = status.lastSyncTime ? `${status.lastSyncDurationMs} ms` : 'N/A';

            if (this.elements.historyContent) {
                let historyHtml = '<table><thead><tr><th>时间</th><th>状态</th><th>耗时 (ms)</th><th>信息</th></tr></thead><tbody>';
                if(status.history && status.history.length > 0) {
                    status.history.forEach(entry => {
                        historyHtml += `
                            <tr>
                                <td>${new Date(entry.timestamp).toLocaleString()}</td>
                                <td>${entry.status}</td>
                                <td>${entry.durationMs}</td>
                                <td>${entry.message || ''}</td>
                            </tr>
                        `;
                    });
                } else {
                    historyHtml += '<tr><td colspan="4" style="text-align:center; padding:1rem;">没有历史记录。</td></tr>';
                }
                historyHtml += '</tbody></table>';
                this.elements.historyContent.innerHTML = historyHtml;
            }

        } catch (error) {
            console.error("Failed to update sync status:", error);
        }
    },

    setupSse() {
        if (this.sseSource) {
            this.sseSource.close();
        }
        this.sseSource = new EventSource(`/api/sync/logs/subscribe/${this.connId}`);
        this.sseSource.onmessage = (event) => {
            if (this.elements.liveLogContent) {
                this.elements.liveLogContent.textContent += event.data + '\n';
                const wrapper = this.elements.liveLogWrapper;
                wrapper.scrollTop = wrapper.scrollHeight;
            }
        };
        this.sseSource.onerror = () => {
            if (this.elements.liveLogContent) {
                this.elements.liveLogContent.textContent += '--- 日志流连接断开, 尝试重连... ---\n';
            }
        };
    },

    closeSse() {
        if (this.sseSource) {
            this.sseSource.close();
            this.sseSource = null;
        }
    }
};

export { syncManager };

