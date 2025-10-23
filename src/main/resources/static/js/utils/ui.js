import { api } from '../services/apiService.js';

let detailsPanelVisible = false;

const ui = {
    elements: {},

    init() {
        this.elements = {
            mainContent: document.getElementById('mainContent'),
            // Modals
            connectionModal: document.getElementById('connectionModal'),
            aiPromptModal: document.getElementById('aiPromptModal'),
            alertModal: document.getElementById('alertModal'),
            confirmModal: document.getElementById('confirmModal'),
            // Toasts
            toast: document.getElementById('toast'),
            // Panel Toggles
            rightPanelToggleBtn: document.getElementById('rightPanelToggleBtn'),
            sidebarCollapseBtn: document.getElementById('sidebarCollapseBtn'),
            leftPanelExpandBtn: document.getElementById('leftPanelExpandBtn'),
            loadingOverlay: document.getElementById('loadingOverlay')
        };

        // --- [修正 v2.3] 将全局事件监听器移入 init() 并添加 null 检查 ---

        // 仅在 connectionModal 存在时绑定事件
        const closeModalBtn = document.getElementById('closeModalBtn');
        if (closeModalBtn) {
            closeModalBtn.addEventListener('click', () => ui.hideConnectionModal());
        }
        if (this.elements.connectionModal) {
            this.elements.connectionModal.addEventListener('click', (e) => {
                if (e.target.id === 'connectionModal') ui.hideConnectionModal();
            });
        }

        // 仅在 alertModal 存在时绑定事件
        const alertOkBtn = document.getElementById('alertOkBtn');
        if (alertOkBtn) {
            alertOkBtn.addEventListener('click', () => {
                if(ui.elements.alertModal) ui.elements.alertModal.style.display = 'none';
            });
        }

        // 仅在 confirmModal 存在时绑定事件
        const confirmCancelBtn = document.getElementById('confirmCancelBtn');
        if (confirmCancelBtn) {
            confirmCancelBtn.addEventListener('click', () => {
                if(ui.elements.confirmModal) ui.elements.confirmModal.style.display = 'none';
            });
        }

        // 仅在 aiPromptModal 存在时绑定事件
        const closeAiModalBtn = document.getElementById('closeAiModalBtn');
        if (closeAiModalBtn) {
            closeAiModalBtn.addEventListener('click', () => ui.hideAiPromptModal());
        }
        const maximizeAiModalBtn = document.getElementById('maximizeAiModalBtn');
        if (maximizeAiModalBtn) {
            maximizeAiModalBtn.addEventListener('click', () => ui.toggleMaximizeAiModal());
        }
        if (this.elements.aiPromptModal) {
            this.elements.aiPromptModal.addEventListener('click', (e) => {
                if (e.target.id === 'aiPromptModal') ui.hideAiPromptModal();
            });
        }
        // --- 修正结束 ---
    },

    showToast(message, type = 'success') {
        if (!this.elements.toast) return; // 检查 toast 元素是否存在
        this.elements.toast.textContent = message;
        this.elements.toast.className = `toast show ${type}`;
        setTimeout(() => {
            if (this.elements.toast) {
                this.elements.toast.className = 'toast';
            }
        }, 3000);
    },

    showLoading() {
        if (this.elements.loadingOverlay) {
            this.elements.loadingOverlay.style.display = 'flex';
        }
    },

    hideLoading() {
        if (this.elements.loadingOverlay) {
            this.elements.loadingOverlay.style.display = 'none';
        }
    },

    showAlert(message, title = "提示") {
        if (!this.elements.alertModal) { // 回退
            console.warn("Alert Modal not found, using console.warn:", message);
            return;
        }
        document.getElementById('alertTitle').textContent = title;
        document.getElementById('alertMessage').textContent = message;
        this.elements.alertModal.style.display = 'flex';
    },

    showConfirm(message, onConfirm, title = "请确认") {
        if (!this.elements.confirmModal) { // 回退
            console.warn("Confirm Modal not found, auto-confirming:", message);
            if (onConfirm) onConfirm();
            return;
        }
        document.getElementById('confirmTitle').textContent = title;
        document.getElementById('confirmMessage').textContent = message;

        const confirmOkBtn = document.getElementById('confirmOkBtn');
        // 替换按钮以移除旧的监听器
        const newOkBtn = confirmOkBtn.cloneNode(true);
        confirmOkBtn.parentNode.replaceChild(newOkBtn, confirmOkBtn);

        newOkBtn.addEventListener('click', () => {
            if (onConfirm) onConfirm();
            this.elements.confirmModal.style.display = 'none';
        });

        this.elements.confirmModal.style.display = 'flex';
    },

    showConnectionModal(connData) {
        if (!this.elements.connectionModal) return;
        const modal = this.elements.connectionModal;
        const title = modal.querySelector('#modalTitle');

        if (connData) {
            title.textContent = '编辑连接';
            modal.querySelector('#connectionId').value = connData.id;
            modal.querySelector('#connectionName').value = connData.name;
            modal.querySelector('#dbType').value = connData.type;
            modal.querySelector('#dbHost').value = connData.host;
            modal.querySelector('#dbPort').value = connData.port;
            modal.querySelector('#dbName').value = connData.databaseName;
            modal.querySelector('#dbUsername').value = connData.username;
            modal.querySelector('#dbPassword').value = connData.password;
        } else {
            title.textContent = '添加新连接';
            modal.querySelector('form').reset();
            modal.querySelector('#connectionId').value = '';
        }
        modal.style.display = 'flex';
    },

    hideConnectionModal() {
        if (this.elements.connectionModal) {
            this.elements.connectionModal.style.display = 'none';
        }
    },

    showAiPromptModal(objectName, details, onSave) {
        if (!this.elements.aiPromptModal) return;
        const modal = this.elements.aiPromptModal;
        const topicInput = modal.querySelector('#aiPromptTopicInput');
        const promptContentEl = modal.querySelector('#aiPromptContent');

        topicInput.value = `关于 ${objectName} 的分析`;

        let context = `表: ${objectName}`;
        if (details.tableComment) context += ` (${details.tableComment})\n\n`;
        context += "字段列表:\n";
        if (details.columns && details.columns.length > 0) {
            details.columns.forEach(col => {
                context += `- ${col.name} (${col.type}${col.size > 0 ? '('+col.size+')' : ''})`;
                if (col.comment) context += `: ${col.comment}`;
                context += '\n';
            });
        }
        promptContentEl.value = context;

        const relatedContainer = modal.querySelector('#aiRelatedInfoContainer');
        relatedContainer.innerHTML = '';
        if (details.relatedObjectGroups && details.relatedObjectGroups.length > 0) {
            details.relatedObjectGroups.forEach(group => {
                if (group.objects && group.objects.length > 0) {
                    const groupDiv = document.createElement('div');
                    groupDiv.className = 'related-group';
                    const title = document.createElement('h5');
                    title.textContent = group.type;
                    const ul = document.createElement('ul');
                    group.objects.forEach(obj => {
                        const li = document.createElement('li');
                        li.textContent = `${obj.name} - ${obj.comment || obj.type}`;
                        ul.appendChild(li);
                    });
                    groupDiv.appendChild(title);
                    groupDiv.appendChild(ul);
                    relatedContainer.appendChild(groupDiv);
                }
            });
        }
        if (relatedContainer.innerHTML === '') {
            relatedContainer.innerHTML = '<p>无相关信息。</p>';
        }

        const saveBtn = document.getElementById('saveAiPromptBtn');
        const newSaveBtn = saveBtn.cloneNode(true);
        saveBtn.parentNode.replaceChild(newSaveBtn, saveBtn);

        newSaveBtn.addEventListener('click', async () => {
            const topic = topicInput.value.trim();
            const prompt = promptContentEl.value.trim();
            if (!topic || !prompt) {
                ui.showAlert("主题和提示内容不能为空！");
                return;
            }
            try {
                await api.saveAiPrompt({ topic, prompt });
                ui.showToast(`主题 "${topic}" 已保存！`);
                ui.hideAiPromptModal();
                if (onSave) {
                    onSave(); // 执行回调
                }
            } catch (error) {
                ui.showToast('保存失败: ' + error.message, 'error');
            }
        });

        modal.style.display = 'flex';
    },

    hideAiPromptModal() {
        if (this.elements.aiPromptModal) {
            this.elements.aiPromptModal.style.display = 'none';
        }
    },

    toggleMaximizeAiModal() {
        if (!this.elements.aiPromptModal) return;
        const modalContent = this.elements.aiPromptModal.querySelector('.modal-content');
        const icon = this.elements.aiPromptModal.querySelector('#maximizeAiModalBtn i');
        modalContent.classList.toggle('maximized');

        if (modalContent.classList.contains('maximized')) {
            icon.className = 'fas fa-compress-arrows-alt';
        } else {
            icon.className = 'fas fa-expand-arrows-alt';
        }
    },

    toggleSidebar(collapse) {
        if (this.elements.mainContent) {
            this.elements.mainContent.classList.toggle('sidebar-collapsed', collapse);
        }
    },

    toggleDetailsPanel(forceState) {
        if (!this.elements.mainContent) return;

        if (typeof forceState === 'boolean') {
            detailsPanelVisible = forceState;
        } else {
            detailsPanelVisible = !detailsPanelVisible;
        }
        this.elements.mainContent.classList.toggle('details-visible', detailsPanelVisible);

        if (this.elements.rightPanelToggleBtn) {
            const icon = this.elements.rightPanelToggleBtn.querySelector('i');
            if (icon) {
                icon.className = detailsPanelVisible ? 'fas fa-chevron-right' : 'fas fa-chevron-left';
            }
        }
    }
};

export { ui };

