import { api } from '../services/apiService.js';
import { ui } from '../utils/ui.js';
import { debounce } from '../utils/helpers.js';

const sqlEditor = {
    sqlTable: null,
    currentSql: '',
    currentConnectionId: null,
    elements: {},
    layoutsCache: [],

    init(container, connId) {
        this.container = container;
        this.currentConnectionId = connId;
        this.render();
        this.loadLayoutsForCurrentSql();
    },

    // 【修复点 1】: 添加 destroy 方法，用于在切换连接时彻底清理
    destroy() {
        if (this.sqlTable) {
            this.sqlTable.destroy();
            this.sqlTable = null;
        }
        // 清理DOM，防止内存泄露
        if (this.container) {
            this.container.innerHTML = '';
        }
    },

    render() {
        this.container.innerHTML = this.getHtmlTemplate();
        this.elements = {
            sqlEditorEl: this.container.querySelector('#sqlEditor'),
            toggleEditorBtn: this.container.querySelector('#toggleEditorBtn'),
            executeSqlBtn: this.container.querySelector('#executeSqlBtn'),
            saveSqlBtn: this.container.querySelector('#saveSqlBtn'),
            loadSqlBtn: this.container.querySelector('#loadSqlBtn'),
            exportExcelBtn: this.container.querySelector('#exportExcelBtn'),
            queryInfoEl: this.container.querySelector('#queryInfo'),
            layoutSelectEl: this.container.querySelector('#layoutSelect'),
            saveLayoutBtn: this.container.querySelector('#saveLayoutBtn'),
            deleteLayoutBtn: this.container.querySelector('#deleteLayoutBtn'),
            columnFilterInput: this.container.querySelector('#columnFilterInput'),
            columnVisibilityList: this.container.querySelector('#columnVisibilityList'),
            // 修正：这里应该是表格的直接容器
            sqlResultContainer: this.container.querySelector('#sqlResult')
        };
        this.addEventListeners();
    },

    getHtmlTemplate() {
        return `
            <div class="sql-editor-area">
                <div class="editor-toolbar">
                    <button id="executeSqlBtn" class="btn btn-primary" style="width: auto; margin-top:0;"><i class="fas fa-play"></i> 执行</button>
                    <button id="saveSqlBtn" class="btn btn-secondary" style="width: auto; margin-top:0;"><i class="fas fa-save"></i> 保存SQL</button>
                    <button id="loadSqlBtn" class="btn btn-secondary" style="width: auto; margin-top:0;"><i class="fas fa-folder-open"></i> 加载SQL</button>
                    <button class="btn btn-secondary" style="width: auto; margin-top:0;" onclick="alert('此功能正在开发中')"><i class="fas fa-file-code"></i> 从D5视图加载</button>
                    <button id="toggleEditorBtn" class="btn btn-secondary" style="width: auto; margin-top:0; margin-left: auto;" title="收起/展开"><i class="fas fa-arrows-alt-v"></i></button>
                </div>
                <div id="sqlEditorWrapper">
                    <textarea id="sqlEditor" placeholder="在此输入 SQL 查询..."></textarea>
                </div>
                <div id="sqlResultContainer">
                     <div id="sqlResultToolbar">
                         <div class="dropdown">
                            <button class="btn btn-secondary" style="width:auto; margin-top:0;"><i class="fas fa-eye"></i> 显示列</button>
                            <div class="dropdown-content">
                                <input type="text" id="columnFilterInput" placeholder="过滤列...">
                                <div id="columnVisibilityList"></div>
                            </div>
                         </div>
                         <button id="saveLayoutBtn" class="btn btn-secondary" style="width:auto; margin-top:0;"><i class="fas fa-save"></i> 另存为布局...</button>
                         <button id="exportExcelBtn" class="btn btn-success" style="width:auto; margin-top:0;"><i class="fas fa-file-excel"></i> 导出Excel</button>
                         <select id="layoutSelect" style="margin-left:auto;"></select>
                         <button id="deleteLayoutBtn" class="btn btn-danger" style="width:auto; margin-top:0; display:none;"><i class="fas fa-trash"></i></button>
                         <span id="queryInfo" style="font-size:0.9em; color:#718096; white-space:nowrap;"></span>
                     </div>
                     <div id="sqlResult"></div>
                </div>
            </div>
        `;
    },

    addEventListeners() {

        this.elements.toggleEditorBtn.addEventListener('click', () => {
            const wrapper = this.elements.sqlEditorEl.closest('#sqlEditorWrapper');
            const icon = this.elements.toggleEditorBtn.querySelector('i');
            wrapper.classList.toggle('collapsed');
            icon.classList.toggle('fa-arrows-alt-v');
            icon.classList.toggle('fa-compress-arrows-alt');
        });
        this.elements.executeSqlBtn.addEventListener('click', () => this.executeSql(1));
        this.elements.saveSqlBtn.addEventListener('click', this.saveSqlToFile.bind(this));
        this.elements.loadSqlBtn.addEventListener('click', () => {
            const fileInput = document.createElement('input');
            fileInput.type = 'file';
            fileInput.accept = '.sql';
            fileInput.onchange = (e) => this.loadSqlFromFile(e);
            fileInput.click();
        });
        this.elements.exportExcelBtn.addEventListener('click', this.exportToExcel.bind(this));
        this.elements.saveLayoutBtn.addEventListener('click', this.saveLayoutAs.bind(this));
        this.elements.deleteLayoutBtn.addEventListener('click', this.deleteSelectedLayout.bind(this));
        this.elements.layoutSelectEl.addEventListener('change', () => {
            const selectedLayout = this.layoutsCache.find(l => l.id === this.elements.layoutSelectEl.value);
            if (selectedLayout) {
                this.executeSql(1);
            }
        });
        this.elements.sqlEditorEl.addEventListener('input', debounce(() => this.loadLayoutsForCurrentSql(), 500));
        this.elements.columnFilterInput.addEventListener('input', this.filterColumnVisibilityList.bind(this));
    },

    async loadLayoutsForCurrentSql() {
        const sql = this.elements.sqlEditorEl.value.trim();
        if (!sql) {
            this.updateLayoutDropdown([]);
            return;
        }

        try {
            const sqlHash = md5(sql.toLowerCase());
            const layouts = await api.getLayoutsForSql(sqlHash);
            this.layoutsCache = layouts || [];
            this.updateLayoutDropdown(this.layoutsCache);
        } catch (error) {
            console.error("Failed to load layouts for SQL", error);
            this.updateLayoutDropdown([]);
        }
    },

    updateLayoutDropdown(layouts) {
        const { layoutSelectEl, deleteLayoutBtn } = this.elements;
        layoutSelectEl.innerHTML = '';
        deleteLayoutBtn.style.display = 'none';

        const namedLayouts = layouts.filter(l => l.name !== null).sort((a, b) => a.name.localeCompare(b.name));
        const autoSavedLayout = layouts.find(l => l.name === null);

        if (namedLayouts.length > 0) {
            namedLayouts.forEach(layout => {
                const option = document.createElement('option');
                option.value = layout.id;
                option.textContent = layout.name;
                layoutSelectEl.appendChild(option);
            });
            layoutSelectEl.value = namedLayouts[0].id;
        } else if (autoSavedLayout) {
            const option = document.createElement('option');
            option.value = autoSavedLayout.id;
            option.textContent = '自动保存的布局';
            layoutSelectEl.appendChild(option);
        } else {
            const option = document.createElement('option');
            option.textContent = '无可用布局';
            option.value = '';
            layoutSelectEl.appendChild(option);
        }

        const selectedLayout = layouts.find(l => l.id === layoutSelectEl.value);
        if (selectedLayout && selectedLayout.name) {
            deleteLayoutBtn.style.display = 'inline-flex';
        }
    },

    async executeSql2(page) {
        const sql = this.elements.sqlEditorEl.value.trim();
        if (!sql) return;

        if (sql !== this.currentSql) {
            if (this.sqlTable) {
                this.sqlTable.destroy();
                this.sqlTable = null;
            }
            this.currentSql = sql;
            page = 1;
        }

        this.elements.executeSqlBtn.disabled = true;
        this.elements.executeSqlBtn.innerHTML = `<i class="fas fa-spinner fa-spin"></i> 执行中`;

        try {
            const selectedLayoutId = this.elements.layoutSelectEl.value;
            const layoutToApply = this.layoutsCache.find(l => l.id === selectedLayoutId) || null;

            const result = await api.executeQuery(this.currentConnectionId, this.currentSql, page - 1, 100);

            if (result.error) {
                if (this.sqlTable) {
                    this.sqlTable.destroy();
                    this.sqlTable = null;
                }
                // 使用正确的容器显示错误
                this.elements.sqlResultContainer.innerHTML = `<p style="padding: 1rem; color: var(--red-color);">${result.error}</p>`;
                this.elements.columnVisibilityList.innerHTML = '';
                this.elements.queryInfoEl.textContent = '';
                return;
            }

            const tableData = {
                data: result.rows,
                last_page: result.totalPages,
            };

            if (!this.sqlTable) {
                // 如果之前有错误信息，确保重新渲染整个表格区域
                if (!this.container.querySelector('#sqlResult')) {
                    this.container.querySelector('#sqlResultContainer').innerHTML = '<div id="sqlResult"></div>';
                    this.elements.sqlResultContainer = this.container.querySelector('#sqlResult');
                }
                this.initializeSqlTable(result, layoutToApply);
            }

            await this.sqlTable.setData(tableData.data);
            this.sqlTable.setMaxPage(result.totalPages);
            await this.sqlTable.setPage(page);

            this.elements.queryInfoEl.textContent = `总计 ${result.totalRows} 行`;
        } catch (e) {
            console.error("Error executing SQL or rendering table:", e);

            // 【修复点 2】: 在 catch 块中添加销毁逻辑
            if (this.sqlTable) {
                this.sqlTable.destroy();
                this.sqlTable = null;
            }
            this.elements.sqlResultContainer.innerHTML = `<p style="padding: 1rem; color: var(--red-color);">执行查询时发生客户端错误。</p>`;
        }
        finally {
            this.elements.executeSqlBtn.disabled = false;
            this.elements.executeSqlBtn.innerHTML = `<i class="fas fa-play"></i> 执行`;
        }
    },

    async executeSql(page) {
        const sql = this.elements.sqlEditorEl.value.trim();
        console.log('[executeSql] Starting execution', {
            sql,
            page,
            currentSql: this.currentSql,
            sqlTableExists: !!this.sqlTable,
        });

        if (!sql) {
            console.log('[executeSql] SQL is empty, exiting');
            return;
        }

        if (sql !== this.currentSql) {
            console.log('[executeSql] SQL changed, destroying existing table', { previousSql: this.currentSql });
            if (this.sqlTable) {
                this.sqlTable.destroy();
                this.sqlTable = null;
            }
            this.currentSql = sql;
            page = 1; // 重置为第一页
        }

        this.elements.executeSqlBtn.disabled = true;
        this.elements.executeSqlBtn.innerHTML = `<i class="fas fa-spinner fa-spin"></i> 执行中`;

        try {
            const selectedLayoutId = this.elements.layoutSelectEl.value;
            console.log('[executeSql] Selected layout ID', { selectedLayoutId, layoutsCache: this.layoutsCache });
            const layoutToApply = this.layoutsCache.find(l => l.id === selectedLayoutId) || null;

            // 如果表格不存在，初始化它（但不加载数据，让 Tabulator 自动处理）
            if (!this.sqlTable) {
                console.log('[executeSql] Initializing new table', { sqlResultContainer: this.elements.sqlResultContainer });
                if (!this.container.querySelector('#sqlResult')) {
                    console.log('[executeSql] Re-rendering sqlResultContainer');
                    this.container.querySelector('#sqlResultContainer').innerHTML = '<div id="sqlResult"></div>';
                    this.elements.sqlResultContainer = this.container.querySelector('#sqlResult');
                }
                // 先执行 API 查询以获取 columnInfo（用于列定义）
                console.log('[executeSql] Executing initial query for columnInfo', {
                    connectionId: this.currentConnectionId,
                    sql,
                    page: 0,
                    pageSize: 100,
                });
                const initialResult = await api.executeQuery(this.currentConnectionId, this.currentSql, 0, 100);
                console.log('[executeSql] Initial query result for columnInfo', {
                    rows: initialResult.rows?.length,
                    totalPages: initialResult.totalPages,
                    totalRows: initialResult.totalRows,
                    error: initialResult.error,
                });

                if (initialResult.error) {
                    console.log('[executeSql] Initial query returned error', { error: initialResult.error });
                    this.elements.sqlResultContainer.innerHTML = `<p style="padding: 1rem; color: var(--red-color);">${initialResult.error}</p>`;
                    this.elements.columnVisibilityList.innerHTML = '';
                    this.elements.queryInfoEl.textContent = '';
                    return;
                }

                this.initializeSqlTable(initialResult, layoutToApply);
                // 等待 tableBuilt，然后触发第一页加载
                await new Promise(resolve => {
                    const onBuilt = () => {
                        console.log('[executeSql] Table built, loading initial page');
                        this.sqlTable.off("tableBuilt", onBuilt); // 移除监听器
                        resolve();
                    };
                    this.sqlTable.on("tableBuilt", onBuilt);
                });
                // 触发第一页加载（Tabulator 会调用 ajaxRequestFunc）
                await this.sqlTable.setPage(1);
            } else {
                // 如果表格已存在且 SQL 未变，触发指定页加载
                console.log('[executeSql] Table exists, loading page', { page });
                await this.sqlTable.setPage(page);
            }

            console.log('[executeSql] Execution completed successfully');
        } catch (e) {
            console.error('[executeSql] Error executing SQL or rendering table', { error: e, stack: e.stack });
            if (this.sqlTable) {
                console.log('[executeSql] Destroying table due to error');
                this.sqlTable.destroy();
                this.sqlTable = null;
            }
            this.elements.sqlResultContainer.innerHTML = `<p style="padding: 1rem; color: var(--red-color);">执行查询时发生客户端错误：${e.message}</p>`;
        } finally {
            this.elements.executeSqlBtn.disabled = false;
            this.elements.executeSqlBtn.innerHTML = `<i class="fas fa-play"></i> 执行`;
        }
    },

    initializeSqlTable(data, layout) {
        console.log('[initializeSqlTable] Starting table initialization', {
            data: { rows: data.rows?.length, columnInfo: data.columnInfo?.length },
            layout,
            sqlResultContainer: this.elements.sqlResultContainer,
        });

        const columnDefs = (data.columnInfo || []).map(col => ({
            title: (col.comment && col.comment.trim()) ? `${col.name} (${col.comment})` : col.name,
            field: col.name,
            headerTooltip: col.comment || col.name,
            resizable: true,
            headerSort: false,
        }));
        console.log('[initializeSqlTable] Column definitions created', { columnDefs: columnDefs.length });

        if (layout && layout.columns) {
            console.log('[initializeSqlTable] Applying layout', { layoutColumns: layout.columns });
            const validFields = new Set(data.columnInfo.map(col => col.name));
            const validLayoutColumns = layout.columns.filter(col => validFields.has(col.field));
            if (validLayoutColumns.length !== layout.columns.length) {
                console.warn('[initializeSqlTable] Some layout columns are invalid or missing', {
                    invalidColumns: layout.columns.filter(col => !validFields.has(col.field)),
                    validFields: Array.from(validFields),
                });
            }

            if (validLayoutColumns.length > 0) {
                const layoutMap = new Map(validLayoutColumns.map(c => [c.field, c]));
                columnDefs.sort((a, b) => {
                    const indexA = validLayoutColumns.findIndex(c => c.field === a.field);
                    const indexB = validLayoutColumns.findIndex(c => c.field === b.field);
                    if (indexA === -1 && indexB === -1) return 0;
                    if (indexA === -1) return 1;
                    if (indexB === -1) return -1;
                    return indexA - indexB;
                });
                columnDefs.forEach(def => {
                    const colLayout = layoutMap.get(def.field);
                    if (colLayout) {
                        def.width = colLayout.width;
                        def.visible = colLayout.visible !== false; // 默认可见
                    }
                });
                console.log('[initializeSqlTable] Column definitions sorted and updated with layout', { columnDefs: columnDefs.length });
            }
        }

        console.log('[initializeSqlTable] Creating new Tabulator instance with remote pagination');
        this.sqlTable = new Tabulator(this.elements.sqlResultContainer, {
            // 不设置初始 data，让 ajaxRequestFunc 处理
            columns: columnDefs,
            layout: "fitDataFill",
            movableColumns: true,
            pagination: true,
            paginationMode: "remote",
            paginationSize: 100,
            paginationInitialPage: 1,
            // 虚拟 URL，实际由 ajaxRequestFunc 处理
            ajaxURL: "remote-data",
            // 自定义请求函数：调用 api.executeQuery
            ajaxRequestFunc: (url, config, params) => {
                console.log('[initializeSqlTable] Ajax request triggered', { url, params });
                return new Promise((resolve, reject) => {
                    api.executeQuery(this.currentConnectionId, this.currentSql, params.page - 1, params.size)
                        .then(result => {
                            console.log('[initializeSqlTable] Ajax request result', {
                                rows: result.rows?.length,
                                totalPages: result.totalPages,
                                totalRows: result.totalRows,
                                error: result.error,
                            });
                            if (result.error) {
                                reject(new Error(result.error));
                            } else {
                                resolve(result);
                            }
                        })
                        .catch(err => {
                            console.error('[initializeSqlTable] Ajax request error', err);
                            reject(err);
                        });
                });
            },
            // 处理响应：返回 data 和 last_page，更新 UI
            ajaxResponse: (url, params, response) => {
                console.log('[initializeSqlTable] Ajax response processed', { params, response });
                // 更新查询信息
                this.elements.queryInfoEl.textContent = `总计 ${response.totalRows} 行 (第 ${params.page} 页 / ${response.totalPages} 页)`;
                // 更新列可见性（如果需要）
                this.updateColumnVisibilityCheckboxes();
                return {
                    data: response.rows || [],
                    last_page: response.totalPages || 1, // 这会正确更新分页控件
                };
            },
            // 错误处理
            ajaxError: (error) => {
                console.error('[initializeSqlTable] Ajax error', error);
                this.elements.sqlResultContainer.innerHTML = `<p style="padding: 1rem; color: var(--red-color);">加载数据时发生错误：${error}</p>`;
            },
        });
        console.log('[initializeSqlTable] Tabulator instance created', { sqlTable: this.sqlTable });

        // 事件绑定
        this.sqlTable.on("pageClick", (e, page) => {
            console.log('[initializeSqlTable] Page click event', { page });
            // Tabulator 会自动处理，无需手动 executeSql
        });

        this.sqlTable.on("tableBuilt", () => {
            console.log('[initializeSqlTable] Table built, updating column visibility checkboxes');
            this.updateColumnVisibilityCheckboxes();
        });
        this.sqlTable.on("columnMoved", () => {
            console.log('[initializeSqlTable] Column moved, auto-saving layout');
            this.autoSaveLayout();
        });
        this.sqlTable.on("columnResized", () => {
            console.log('[initializeSqlTable] Column resized, auto-saving layout');
            this.autoSaveLayout();
        });
        this.sqlTable.on("columnVisibilityChanged", () => {
            console.log('[initializeSqlTable] Column visibility changed, updating checkboxes and auto-saving layout');
            this.autoSaveLayout();
            this.updateColumnVisibilityCheckboxes();
        });
    },

    initializeSqlTable2(data, layout) {
        const columnDefs = data.columnInfo.map(col => ({
            title: (col.comment && col.comment.trim()) ? `${col.name} (${col.comment})` : col.name,
            field: col.name,
            headerTooltip: col.comment || col.name,
            resizable: true,
            headerSort: false,
        }));

        if (layout && layout.columns) {
            const layoutMap = new Map(layout.columns.map(c => [c.field, c]));
            columnDefs.sort((a, b) => {
                const indexA = layout.columns.findIndex(c => c.field === a.field);
                const indexB = layout.columns.findIndex(c => c.field === b.field);
                if (indexA === -1 && indexB === -1) return 0;
                if (indexA === -1) return 1;
                if (indexB === -1) return -1;
                return indexA - indexB;
            });
            columnDefs.forEach(def => {
                const colLayout = layoutMap.get(def.field);
                if (colLayout) {
                    def.width = colLayout.width;
                    def.visible = colLayout.visible;
                }
            });
        }

        this.sqlTable = new Tabulator(this.elements.sqlResultContainer, {
            data: data.rows,
            columns: columnDefs,
            layout: "fitDataFill",
            movableColumns: true,
            pagination: true,
            paginationMode: "remote",
            paginationSize: 100,
            paginationInitialPage: 1,
        });

        this.sqlTable.on("pageClick", (e, page) => {
            this.executeSql(page);
        });

        this.sqlTable.on("tableBuilt", () => this.updateColumnVisibilityCheckboxes());
        this.sqlTable.on("columnMoved", () => this.autoSaveLayout());
        this.sqlTable.on("columnResized", () => this.autoSaveLayout());
        this.sqlTable.on("columnVisibilityChanged", () => {
            this.autoSaveLayout();
            this.updateColumnVisibilityCheckboxes();
        });
    },

    // 其他方法保持不变...
    updateColumnVisibilityCheckboxes() {
        if (!this.sqlTable) return;
        const { columnVisibilityList } = this.elements;
        columnVisibilityList.innerHTML = '';
        this.sqlTable.getColumns().forEach(column => {
            const def = column.getDefinition();
            const label = document.createElement('label');
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.checked = column.isVisible();
            checkbox.onchange = () => column.toggle();
            label.appendChild(checkbox);
            label.appendChild(document.createTextNode(` ${def.title}`));
            columnVisibilityList.appendChild(label);
        });
    },

    filterColumnVisibilityList() {
        const filterValue = this.elements.columnFilterInput.value.toLowerCase();
        const labels = this.elements.columnVisibilityList.querySelectorAll('label');
        labels.forEach(label => {
            label.style.display = label.textContent.toLowerCase().includes(filterValue) ? 'block' : 'none';
        });
    },

    autoSaveLayout() {
        if (!this.sqlTable || !this.currentSql) return;

        const selectedId = this.elements.layoutSelectEl.value;
        const selectedLayout = this.layoutsCache.find(l => l.id === selectedId);
        const layoutName = selectedLayout ? selectedLayout.name : null;

        const columns = this.sqlTable.getColumns().map(c => ({
            field: c.getField(),
            width: c.getWidth(),
            visible: c.isVisible(),
        }));
        const sqlHash = md5(this.currentSql.trim().toLowerCase());

        const layoutToSave = { sqlHash, columns, name: layoutName };

        api.saveLayout(layoutToSave).then(savedLayout => {
            const oldLayoutIndex = this.layoutsCache.findIndex(l => (layoutName === null) ? l.name === null : l.id === selectedId);
            if (oldLayoutIndex > -1) this.layoutsCache.splice(oldLayoutIndex, 1);
            this.layoutsCache.push(savedLayout);
            this.updateLayoutDropdown(this.layoutsCache);
            this.elements.layoutSelectEl.value = savedLayout.id;

            const toastMessage = layoutName ? `布局 "${layoutName}" 已更新` : '布局已自动保存';
            ui.showToast(toastMessage, 'success');
        });
    },

    async saveLayoutAs() {
        if (!this.sqlTable || !this.currentSql) {
            return ui.showAlert("请先执行一个查询以生成布局。");
        }
        const name = prompt("请输入布局名称：");
        if (name && name.trim()) {
            const columns = this.sqlTable.getColumns().map(c => ({
                field: c.getField(),
                width: c.getWidth(),
                visible: true,
            }));
            const sqlHash = md5(this.currentSql.trim().toLowerCase());
            const layout = { sqlHash, columns, name: name.trim() };
            const savedLayout = await api.saveLayout(layout);
            const otherLayouts = this.layoutsCache.filter(l => l.name !== name.trim());
            this.layoutsCache = [...otherLayouts, savedLayout];
            this.updateLayoutDropdown(this.layoutsCache);
            this.elements.layoutSelectEl.value = savedLayout.id;
            ui.showToast(`布局 "${name}" 已保存!`);
        }
    },

    async deleteSelectedLayout() {
        const selectedId = this.elements.layoutSelectEl.value;
        const selectedLayout = this.layoutsCache.find(l => l.id === selectedId);

        if (!selectedLayout || !selectedLayout.name) {
            return ui.showAlert("不能删除自动保存的布局。");
        }

        ui.showConfirm(`确定要删除布局 "${selectedLayout.name}" 吗？`, async () => {
            await api.deleteLayout(selectedId);
            ui.showToast(`布局 "${selectedLayout.name}" 已删除。`);
            await this.loadLayoutsForCurrentSql();
        });
    },

    async exportToExcel() {
        if (!this.currentSql) {
            return ui.showAlert("没有可导出的查询。");
        }
        this.elements.exportExcelBtn.disabled = true;
        this.elements.exportExcelBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 导出中...';
        try {
            const blob = await api.exportToExcel(this.currentConnectionId, this.currentSql);
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `query_result_${new Date().getTime()}.xlsx`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();
        } finally {
            this.elements.exportExcelBtn.disabled = false;
            this.elements.exportExcelBtn.innerHTML = '<i class="fas fa-file-excel"></i> 导出Excel';
        }
    },

    saveSqlToFile() {
        const sql = this.elements.sqlEditorEl.value;
        if (!sql.trim()) return;
        const blob = new Blob([sql], { type: 'text/plain;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `query_${new Date().getTime()}.sql`;
        a.click();
        URL.revokeObjectURL(a.href);
    },

    loadSqlFromFile(event) {
        const file = event.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (e) => {
            this.elements.sqlEditorEl.value = e.target.result;
            this.loadLayoutsForCurrentSql();
        };
        reader.readAsText(file);
    },

    setSqlAndExecute(sql) {
        this.elements.sqlEditorEl.value = sql;
        this.loadLayoutsForCurrentSql().then(() => {
            this.executeSql(1);
        });
    },

    setSqlOnly(sql) {
        this.elements.sqlEditorEl.value = sql;
        this.loadLayoutsForCurrentSql();
    }
};

export { sqlEditor };