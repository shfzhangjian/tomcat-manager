import { api } from '../services/apiService.js';

const objectBrowser = {
    container: null,
    connId: null,
    callbacks: null,

    init(containerElement, connId, callbacks) {
        this.container = containerElement;
        this.connId = connId;
        this.callbacks = callbacks;
        this.loadObjectTypeCards();
    },

    async loadObjectTypeCards() {
        this.container.innerHTML = '<div><i class="fas fa-spinner fa-spin"></i> 正在加载对象类型...</div>';
        try {
            const types = await api.getObjectTypes(this.connId);
            let cardsHtml = '<div class="object-browser-grid">';
            types.forEach(type => {
                if (type.count > 0) {
                    cardsHtml += `
                        <div class="object-type-card" data-type="${type.type}" data-name="${type.displayName}">
                            <i class="fas fa-table"></i>
                            <h3>${type.displayName}</h3>
                            <p>${type.count} 个对象</p>
                        </div>
                    `;
                }
            });
            cardsHtml += '</div>';
            this.container.innerHTML = cardsHtml;
            this.container.querySelectorAll('.object-type-card').forEach(card => {
                card.addEventListener('click', () => {
                    this.showObjectListView(card.dataset.type, card.dataset.name);
                });
            });
        } catch (e) {
            this.container.innerHTML = '<div>加载对象失败。</div>';
        }
    },

    showObjectListView(type, displayName, page = 0, filter = '') {
        this.container.innerHTML = `
            <div class="object-list-view">
                <div class="object-list-sticky-header">
                    <div class="object-list-header">
                        <button id="backToCardsBtn" class="btn btn-secondary" style="width:auto; margin-top:0;"><i class="fas fa-arrow-left"></i> 返回</button>
                        <h3 style="margin: 0;">${displayName}</h3>
                    </div>
                    <div class="filter-group">
                        <input type="text" id="objectFilter" value="${filter}" placeholder="过滤对象名称或注释...">
                        <button id="filterBtn" class="btn btn-primary" style="width:auto; margin-top:0; margin-left:0.5rem;"><i class="fas fa-search"></i> 查询</button>
                    </div>
                </div>
                <div id="objectListContainer" class="object-list-container">
                    <div><i class="fas fa-spinner fa-spin"></i> 正在加载...</div>
                </div>
                <div id="paginationControls" class="pagination"></div>
            </div>
        `;
        this.container.querySelector('#backToCardsBtn').addEventListener('click', () => this.loadObjectTypeCards());
        const filterBtn = this.container.querySelector('#filterBtn');
        const filterInput = this.container.querySelector('#objectFilter');
        filterBtn.addEventListener('click', () => {
            this.showObjectListView(type, displayName, 0, filterInput.value);
        });
        filterInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') filterBtn.click();
        });
        this.loadPaginatedObjects(type, displayName, page, filter);
    },

    async loadPaginatedObjects(type, displayName, page, filter) {
        try {
            const result = await api.getObjects(this.connId, type, page, filter);
            this.renderObjectCards(type, displayName, result);
            this.renderPagination(type, displayName, result);
        } catch (e) {
            this.container.querySelector('#objectListContainer').innerHTML = '<div style="padding:1rem; text-align:center;">加载列表失败。</div>';
        }
    },

    renderObjectCards(type, displayName, data) {
        const listContainer = this.container.querySelector('#objectListContainer');
        if (!listContainer) return;
        listContainer.innerHTML = '<div class="object-card-grid"></div>';
        const grid = listContainer.querySelector('.object-card-grid');

        if (data.content.length > 0) {
            data.content.forEach(obj => {
                const card = document.createElement('div');
                card.className = 'object-card';
                card.innerHTML = `
                    <div class="card-header">
                        <h4>${obj.name}</h4>
                        <p>${obj.comment || '&nbsp;'}</p>
                    </div>
                    <div class="card-actions">
                        <button class="btn-text" data-action="details">详情</button>
                        <button class="btn-text" data-action="select">数据</button>
                        <button class="btn-text" data-action="copy">复制</button>
                        <button class="btn-text" data-action="ai">AI训练</button>
                    </div>`;

                card.querySelector('[data-action="details"]').addEventListener('click', () => this.callbacks.onViewDetails(obj.name, type));
                card.querySelector('[data-action="select"]').addEventListener('click', () => this.callbacks.onViewData(obj.name));
                card.querySelector('[data-action="copy"]').addEventListener('click', () => this.callbacks.onCopySql(obj.name));
                card.querySelector('[data-action="ai"]').addEventListener('click', () => this.callbacks.onAiTrain(obj.name, type));

                grid.appendChild(card);
            });
        } else {
            listContainer.innerHTML = '<div style="padding:1rem; text-align:center;">未找到匹配的对象。</div>';
        }
    },

    renderPagination(type, displayName, data) {
        const controls = this.container.querySelector('#paginationControls');
        if (!controls) return;
        controls.innerHTML = '';
        if (data.totalPages <= 1) return;

        const prev = document.createElement('button');
        prev.textContent = '上一页';
        prev.className = "btn btn-secondary";
        prev.style.cssText = 'width:auto; margin-top:0;';
        prev.disabled = data.currentPage === 0;
        prev.onclick = () => this.showObjectListView(type, displayName, data.currentPage - 1, this.container.querySelector('#objectFilter').value);

        const next = document.createElement('button');
        next.textContent = '下一页';
        next.className = "btn btn-secondary";
        next.style.cssText = 'width:auto; margin-top:0;';
        next.disabled = data.currentPage >= data.totalPages - 1;
        next.onclick = () => this.showObjectListView(type, displayName, data.currentPage + 1, this.container.querySelector('#objectFilter').value);

        const text = document.createElement('span');
        text.textContent = `第 ${data.currentPage + 1} / ${data.totalPages} 页 (共 ${data.totalElements} 条)`;
        text.style.margin = "0 1rem";

        controls.appendChild(prev);
        controls.appendChild(text);
        controls.appendChild(next);
    }
};

export { objectBrowser };

