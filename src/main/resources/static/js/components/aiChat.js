import { api } from '../services/apiService.js';
import { ui } from '../utils/ui.js';

const aiChat = {
    container: null,
    connId: null,
    elements: {},
    aiPromptsCache: [],

    init(containerElement, connId) {
        this.container = containerElement;
        this.connId = connId;
        this.render(); // Render the component's HTML first
        this.loadAiPrompts();
    },

    render() {
        this.container.innerHTML = this.getHtmlTemplate();
        this.elements = {
            history: this.container.querySelector('#aiChatHistory'),
            input: this.container.querySelector('#aiQueryInput'),
            sendBtn: this.container.querySelector('#aiSendBtn'),
            promptSelect: this.container.querySelector('#aiPromptSelect')
        };

        this.elements.sendBtn.addEventListener('click', () => this.sendQuery());
        this.elements.input.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') this.sendQuery();
        });
        this.container.querySelectorAll('.prompt-chip').forEach(p => {
            p.addEventListener('click', () => {
                this.elements.input.value = p.textContent;
                this.sendQuery();
            });
        });
    },

    getHtmlTemplate() {
        return `
            <div class="ai-chat-area">
                <div id="aiChatHistory" class="ai-chat-history">
                    <div class="chat-message ai"><div class="chat-message-bubble">您好！我是您的数据助手，请问有什么可以帮您的？您可以试试点击下面的问题。</div></div>
                </div>
                 <div class="form-group" style="flex-shrink:0;">
                     <label for="aiPromptSelect">选择问数主题 (可选):</label>
                     <select id="aiPromptSelect" class="form-group full-width"><option value="">无特定主题</option></select>
                 </div>
                <div class="ai-input-area">
                    <input type="text" id="aiQueryInput" class="form-group full-width" placeholder="例如：查询所有用户">
                    <button id="aiSendBtn" class="btn btn-primary" style="width: auto; margin-top:0;"><i class="fas fa-paper-plane"></i> 发送</button>
                </div>
                <div class="example-prompts">
                    <span class="prompt-chip">查询所有用户</span>
                    <span class="prompt-chip">统计每个部门的员工人数</span>
                </div>
            </div>
        `;
    },

    async loadAiPrompts() {
        try {
            this.aiPromptsCache = await api.getAiPrompts();
            this.elements.promptSelect.innerHTML = '<option value="">无特定主题</option>';
            this.aiPromptsCache.forEach(p => {
                const option = document.createElement('option');
                option.value = p.id;
                option.textContent = p.topic;
                this.elements.promptSelect.appendChild(option);
            });
        } catch (error) {
            console.error("Failed to load AI prompts", error);
        }
    },

    async sendQuery() {
        const query = this.elements.input.value.trim();
        if (!query) return;

        this.addMessageToHistory(query, 'user');
        this.elements.input.value = '';

        const thinkingId = `msg-${Date.now()}`;
        this.addMessageToHistory('<i class="fas fa-spinner fa-spin"></i> 正在思考...', 'ai', thinkingId);

        try {
            const selectedPrompt = this.aiPromptsCache.find(p => p.id === this.elements.promptSelect.value);
            const response = await api.queryAi(query, selectedPrompt ? selectedPrompt.prompt : '');

            const thinkingMessage = document.getElementById(thinkingId);
            if (response && thinkingMessage) {
                const resultTable = this.createResultTable(response.queryResult);
                const bubbleContent = `
                    <p>${response.naturalLanguageResponse}</p>
                    <div class="ai-sql-block">${response.sql}</div>
                    <div style="margin-top: 1rem; color: var(--text-color);">${resultTable}</div>
                `;
                thinkingMessage.querySelector('.chat-message-bubble').innerHTML = bubbleContent;
            } else if (thinkingMessage) {
                thinkingMessage.querySelector('.chat-message-bubble').textContent = '抱歉，处理时遇到错误。';
            }
            this.elements.history.scrollTop = this.elements.history.scrollHeight;
        } catch (error) {
            const thinkingMessage = document.getElementById(thinkingId);
            if (thinkingMessage) {
                thinkingMessage.querySelector('.chat-message-bubble').textContent = '请求AI服务时出错。';
            }
        }
    },

    addMessageToHistory(content, type, id = null) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `chat-message ${type}`;
        if (id) messageDiv.id = id;

        messageDiv.innerHTML = `<div class="chat-message-bubble">${content}</div>`;
        this.elements.history.appendChild(messageDiv);
        this.elements.history.scrollTop = this.elements.history.scrollHeight;
    },

    createResultTable(queryResult) {
        if (queryResult.error) {
            return `<p style="color: var(--red-color);">${queryResult.error}</p>`;
        }
        if (!queryResult.rows || queryResult.rows.length === 0) {
            return '<p>查询成功，但未返回任何行。</p>';
        }

        const headers = queryResult.columnInfo.map(h => `<th>${h.name}</th>`).join('');
        const body = queryResult.rows.map(row => {
            const cells = queryResult.columnInfo.map(h => `<td>${row[h.name] || 'NULL'}</td>`).join('');
            return `<tr>${cells}</tr>`;
        }).join('');

        return `
            <div style="max-height: 200px; overflow: auto;">
                <table>
                    <thead><tr>${headers}</tr></thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        `;
    }
};

export { aiChat };

