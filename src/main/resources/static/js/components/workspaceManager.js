import { sqlEditor } from './sqlEditor.js';
import { objectBrowser } from './objectBrowser.js';
import { aiChat } from './aiChat.js';
import { detailsPanel } from './detailsPanel.js';
import { ui } from '../utils/ui.js';
import { api } from '../services/apiService.js';

const workspaceManager = {
    container: null,
    connId: null,

    init(containerElement) {
        this.container = containerElement;
    },

    render(connId, connName) {
        this.connId = connId;

        if (typeof sqlEditor.destroy === 'function') {
            sqlEditor.destroy();
        }

        this.container.innerHTML = this.getHtmlTemplate(connName);

        const firstTab = this.container.querySelector('.workspace-tab.active');
        if (firstTab) {
            const firstContentId = firstTab.dataset.tab + 'Content';
            const firstContentEl = document.getElementById(firstContentId);
            this.initComponents(firstContentEl, firstContentId, connId);
            firstContentEl.dataset.initialized = "true";
        }

        this.setupEventListeners(connId);
    },

    getHtmlTemplate(connectionName) {
        return `
            <h2 class="workspace-header">${connectionName}</h2>
            <div class="workspace">
                <div class="workspace-tabs">
                    <button class="workspace-tab active" data-tab="objects">对象浏览器</button>
                    <button class="workspace-tab" data-tab="editor">SQL 编辑器</button>
                    <button class="workspace-tab" data-tab="ai">智能问数 (AI)</button>
                </div>
                <div class="workspace-content active" id="objectsContent"></div>
                <div class="workspace-content" id="editorContent"></div>
                <div class="workspace-content" id="aiContent"></div>
            </div>
        `;
    },

    setupEventListeners(connId) {
        const tabs = this.container.querySelectorAll('.workspace-tab');
        tabs.forEach(tab => {
            tab.addEventListener('click', () => {
                tabs.forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                this.container.querySelectorAll('.workspace-content').forEach(c => c.classList.remove('active'));
                const contentId = tab.dataset.tab + 'Content';
                const contentEl = document.getElementById(contentId);
                contentEl.classList.add('active');
                if (!contentEl.dataset.initialized) {
                    this.initComponents(contentEl, contentId, connId);
                    contentEl.dataset.initialized = "true";
                }
            });
        });
    },

    initComponents(container, contentId, connId) {
        if (contentId === 'objectsContent') {
            objectBrowser.init(container, connId, this);
        } else if (contentId === 'editorContent') {
            sqlEditor.init(container, connId);
        } else if (contentId === 'aiContent') {
            aiChat.init(container, connId);
        }
    },

    switchToTab(tabName, context) {
        const tabButton = this.container.querySelector(`.workspace-tab[data-tab="${tabName}"]`);
        if(tabButton) {
            tabButton.click();
            setTimeout(() => {
                if (tabName === 'editor' && context) {
                    if (context.execute) {
                        sqlEditor.setSqlAndExecute(context.sql);
                    } else {
                        sqlEditor.setSqlOnly(context.sql);
                    }
                }
            }, 0);
        }
    },

    onViewDetails(objectName, objectType) {
        detailsPanel.loadDetails(this.connId, objectType, objectName);
    },

    onViewData(objectName) {
        this.switchToTab('editor', { sql: `SELECT * FROM "${objectName}"`, execute: true });
    },

    onCopySql(objectName) {
        this.switchToTab('editor', { sql: `SELECT * FROM "${objectName}"`, execute: false });
        ui.showToast(`查询语句已复制到编辑器`);
    },

    // **【修正点 C】**: 在调用 showAiPromptModal 时传入回调函数
    async onAiTrain(objectName, objectType) {
        try {
            ui.showLoading();
            const details = await api.getObjectDetails(this.connId, objectType, objectName);
            // 传入一个回调，该回调将在保存成功后执行
            ui.showAiPromptModal(objectName, details, () => {
                // 检查aiChat是否已初始化，如果已初始化，则重新加载提示
                if (document.getElementById('aiContent').dataset.initialized === "true") {
                    aiChat.loadAiPrompts();
                }
            });
        } catch (e) {

            console.error("Failed to get details for AI training", e);
            ui.showAlert("获取对象详情失败，无法进行AI训练。");
        } finally {
            ui.hideLoading();
        }
    }
};

export { workspaceManager };