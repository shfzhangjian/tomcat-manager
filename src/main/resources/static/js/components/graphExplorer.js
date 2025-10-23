import { D, debounce } from '../utils/helpers.js';
import { api } from '../services/apiService.js';
// [v2.5 修正] 引入 ui.js
import { ui } from '../utils/ui.js';

// 模块内部变量
let network = null;
let nodes = new vis.DataSet([]);
let edges = new vis.DataSet([]);
let elements = {}; // UI 元素缓存

/**
 * [v2.5 修正] 初始化 vis-network 画布
 */
function initVisNetwork() {
    const container = elements.graphContainer;
    const data = { nodes, edges };
    const visNetworkOptions = {
        nodes: {
            shape: 'dot',
            size: 20,
            font: {
                size: 14,
                color: '#333'
            },
            borderWidth: 2,
        },
        edges: {
            width: 2,
            arrows: 'to',
            color: {
                color: '#848484',
                highlight: '#667eea',
                hover: '#667eea'
            },
            font: {
                align: 'top',
                size: 10,
                color: '#555',
                strokeWidth: 0, // 确保文字清晰
                background: 'rgba(255,255,255,0.7)' // 为文字添加浅色背景
            },
            smooth: {
                type: 'dynamic' // 使用动态平滑
            }
        },
        // [v2.5 修正] 移除鸟瞰图, 保留导航按钮
        interaction: {
            hover: true,
            tooltipDelay: 200,
            navigationButtons: true, // 保留缩放按钮
            keyboard: true
        },
        physics: {
            enabled: true,
            solver: 'forceAtlas2Based', // 换一个更现代的布局算法
            forceAtlas2Based: {
                gravitationalConstant: -50,
                centralGravity: 0.01,
                springLength: 100,
                springConstant: 0.08,
                avoidOverlap: 0.5 // 增加节点间距
            },
            minVelocity: 0.75,
            stabilization: {
                iterations: 150
            }
        }
        // [v2.5 修正] 移除鸟瞰图配置
        // overview: { ... }
    };
    network = new vis.Network(container, data, visNetworkOptions);

    // 绑定画布事件
    network.on("click", (params) => {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            const node = nodes.get(nodeId);
            showNodeDetails(node);
        } else {
            clearNodeDetails();
        }
    });

    network.on("doubleClick", (params) => {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            expandNode(nodeId);
        }
    });

    // [v2.5 新增] 监听稳定化事件
    network.on("stabilizationIterationsDone", () => {
        network.fit(); // 稳定后自动缩放
    });
}

/**
 * 加载图谱 Schema（节点标签、关系类型）
 */
async function loadSchemaInfo() {
    try {
        // [v2.5 新增] 显示加载中
        ui.showLoading();
        const schema = await api.getGraphSchema();
        renderSchemaInfo(schema.labels, elements.schemaNodeLabels, 'label');
        renderSchemaInfo(schema.relationshipTypes, elements.schemaRelTypes, 'relationship');
    } catch (error) {
        console.error("Failed to load graph schema:", error);
        if (elements.schemaNodeLabels) {
            elements.schemaNodeLabels.innerHTML = '<p class="schema-error">加载标签失败</p>';
        }
    } finally {
        ui.hideLoading();
    }
}

/**
 * 渲染 Schema 标签
 */
function renderSchemaInfo(items, container, type) {
    if (!container) return; // 元素不存在则退出
    container.innerHTML = '';
    if (items && items.length > 0) {
        items.forEach(item => {
            const el = document.createElement('div');
            el.className = 'schema-tag';
            el.innerHTML = `<span>${item.name}</span> <span class="schema-count">${item.count}</span>`;

            if (type === 'label') {
                el.addEventListener('click', async () => {
                    elements.searchInput.value = item.name;
                    ui.showLoading();
                    try {
                        const results = await performSearch(item.name);
                        if (results && results.length > 0) {
                            const firstNode = results[0];
                            const nodeLabel = getNodeDisplayLabel(firstNode);
                            loadAndRenderNodeById(firstNode.id, nodeLabel, firstNode);
                        }
                    } catch (error) {
                        console.error("Failed to auto-load node from label click:", error);
                    } finally {
                        ui.hideLoading();
                    }
                });
            }

            container.appendChild(el);
        });
    } else {
        container.innerHTML = `<p class="schema-none">无${type === 'label' ? '节点标签' : '关系类型'}</p>`;
    }
}

/**
 * 执行搜索 (现在返回结果)
 * @param {string} searchTerm
 * @returns {Promise<Array>} 搜索结果数组
 */
async function performSearch(searchTerm) {
    const term = searchTerm || elements.searchInput.value.trim();

    if (!term) {
        if (elements.searchResultsContainer) {
            elements.searchResultsContainer.style.display = 'none';
        }
        return [];
    }

    try {
        const results = await api.searchGraphNodes(term);
        renderSearchResults(results);
        return results;
    } catch (error) {
        console.error("Search failed:", error);
        if (elements.searchResultsContainer) {
            elements.searchResultsContainer.innerHTML = '<div class="search-result-item error">搜索失败</div>';
            elements.searchResultsContainer.style.display = 'block';
        }
        return [];
    }
}

/**
 * 渲染搜索结果
 */
function renderSearchResults(results) {
    if (!elements.searchResultsContainer) return;

    elements.searchResultsContainer.innerHTML = '';
    if (results.length === 0) {
        elements.searchResultsContainer.innerHTML = '<div class="search-result-item">未找到匹配节点</div>';
    } else {
        results.forEach(node => {
            const item = document.createElement('div');
            item.className = 'search-result-item';

            const nodeLabel = getNodeDisplayLabel(node);

            item.innerHTML = `<i class="fas fa-circle" style="color: #667eea;"></i> ${nodeLabel}`;
            item.title = `ID: ${node.id}, 标签: ${node.labels.join(', ')}`;

            item.addEventListener('click', () => {
                loadAndRenderNodeById(node.id, nodeLabel, node);
            });
            elements.searchResultsContainer.appendChild(item);
        });
    }
    elements.searchResultsContainer.style.display = 'block';
}

/**
 * [v2.4 修正] 辅助函数，决定节点上显示什么
 * 优先显示中文 '名称', 'name', 'label' 属性，最后回退到第一个标签
 */
function getNodeDisplayLabel(nodeData) {
    if (!nodeData) return "N/A";
    const props = nodeData.properties || {};

    // 优先使用中文“名称”
    if (props["名称"]) return props["名称"];
    if (props.name) return props.name;
    if (props.label) return props.label; // (虽然 'label' 也是 vis 的一个属性)

    // 回退到第一个标签
    if (nodeData.labels && nodeData.labels.length > 0) {
        // 过滤掉 '设备' 基础标签，显示更具体的标签
        const specificLabel = nodeData.labels.find(l => l !== '设备' && l !== '机组');
        if (specificLabel) return specificLabel;
        return nodeData.labels[0]; // 如果只有 '设备' 或 '机组'
    }
    return String(nodeData.id); // 最终回退
}

/**
 * 加载一个节点并将其添加到图谱中
 * @param {number|string} nodeId 节点ID
 * @param {string} nodeLabel 节点显示名称（来自搜索结果）
 * @param {object} nodeData 完整的节点数据
 */
function loadAndRenderNodeById(nodeId, nodeLabel, nodeData) {
    if (elements.searchResultsContainer) {
        elements.searchResultsContainer.style.display = 'none';
    }
    if (elements.searchInput) {
        elements.searchInput.value = '';
    }

    // 检查节点是否已存在
    const existingNode = nodes.get(nodeId);

    if (!existingNode) {
        const nodeToAdd = {
            id: nodeId,
            label: nodeLabel || String(nodeId),
            title: formatNodeTitle(nodeData),
            properties: nodeData.properties,
            labels: nodeData.labels,
            group: (nodeData.labels && nodeData.labels.length > 0) ? nodeData.labels[0] : 'default'
        };
        nodes.add(nodeToAdd);
        showNodeDetails(nodeToAdd); // [v2.5] 显示新添加节点的详情
    } else {
        // [v2.5] 如果节点已存在，也显示其详情
        showNodeDetails(existingNode);
    }

    // 聚焦到该节点
    network.fit({ nodes: [nodeId], animation: true });
    // 立即展开该节点
    expandNode(nodeId);
}

/**
 * 展开一个节点（加载其邻居和关系）
 * @param {number|string} nodeId
 */
async function expandNode(nodeId) {
    try {
        ui.showLoading();
        const data = await api.expandGraphNode(nodeId);

        if (data && data.nodes && data.edges) {
            renderGraph(data); // 渲染返回的节点和 *关系*
        } else {
            console.warn("expandNode API 返回的数据格式不正确: ", data);
        }
    } catch (error) {
        console.error("Failed to expand node:", error);
    } finally {
        ui.hideLoading();
    }
}

/**
 * [v2.5 修正] 渲染图谱数据（节点和关系）
 */
function renderGraph(data) {
    const newNodes = [];
    data.nodes.forEach(node => {
        if (!nodes.get(node.id)) { // 只添加新节点
            newNodes.push({
                id: node.id,
                label: getNodeDisplayLabel(node),
                title: formatNodeTitle(node),
                properties: node.properties,
                labels: node.labels,
                group: (node.labels && node.labels.length > 0) ? node.labels[0] : 'default'
            });
        }
    });
    if (newNodes.length > 0) {
        nodes.add(newNodes);
    }

    const newEdges = [];
    if (data.edges && data.edges.length > 0) {
        data.edges.forEach(edge => {
            if (!edges.get(edge.id)) { // 只添加新关系
                newEdges.push({
                    id: edge.id,
                    // [v2.5 关键修正] 匹配 GraphService.java 的返回
                    from: edge.startNodeId,
                    to: edge.endNodeId,
                    label: edge.type
                });
            }
        });
        if (newEdges.length > 0) {
            edges.add(newEdges);
        }
    } else {
        if (data.nodes.length > 1) { // 展开返回了新节点，但没有边
            console.warn("expandNode API did not return any edges (lines).");
        }
    }

    // [v2.5] 稳定化布局
    network.stabilize(1000);
}

/**
 * 格式化节点悬停时的提示信息
 */
function formatNodeTitle(node) {
    if (!node) return '';
    let title = `<b>${getNodeDisplayLabel(node)}</b><br>`;
    title += `ID: ${node.id}<br>`;
    if (node.labels) {
        title += `标签: ${node.labels.join(', ')}`;
    }
    return title;
}

/**
 * 在右侧面板显示节点详情
 */
function showNodeDetails(node) {
    if (!elements.detailsPanelEl) return;

    // [v2.5] 自动展开面板
    toggleDetailsPanel(true);

    // [v2.5 修正] node.label 是 vis-network 内部的显示标签, 我们需要原始数据
    const nodeLabel = node.label || getNodeDisplayLabel(node);
    const nodeLabels = node.labels || [];
    const props = node.properties || {};

    let contentHtml = `
        <div class="detail-item">
            <strong>名称</strong>
            <span>${nodeLabel || 'N/A'}</span>
        </div>
        <div class="detail-item">
            <strong>Neo4j ID</strong>
            <span>${node.id}</span>
        </div>
        <div class="detail-item detail-labels">
            <strong>类型 (标签)</strong>
            ${(nodeLabels).map(l => `<span class="label-tag">${l}</span>`).join('') || '<span>无</span>'}
        </div>
    `;

    contentHtml += '<strong>属性</strong>';

    if (Object.keys(props).length === 0) {
        contentHtml += '<p>无属性</p>';
    } else {
        for (const key in props) {
            contentHtml += `
                <div class="detail-item">
                    <strong>${key}</strong>
                    <span>${props[key] === null ? 'null' : props[key]}</span>
                </div>
            `;
        }
    }

    elements.detailsPanelEl.querySelector('#details-title').textContent = nodeLabel || '节点详情';
    elements.detailsPanelEl.querySelector('.details-content-body').innerHTML = contentHtml;
}

/**
 * 清除右侧详情面板
 */
function clearNodeDetails() {
    if (!elements.detailsPanelEl) return;
    elements.detailsPanelEl.querySelector('#details-title').textContent = '节点详情';
    elements.detailsPanelEl.querySelector('.details-content-body').innerHTML = '<p>点击一个节点查看其详情。</p>';
}

/**
 * [v2.5 新增] 切换右侧详情面板的显示
 */
function toggleDetailsPanel(show) {
    const container = D('graph-explorer-container');
    const btnIcon = elements.toggleDetailsBtn.querySelector('i');

    let isCollapsed;

    if (typeof show === 'boolean') {
        isCollapsed = !show;
    } else {
        isCollapsed = !container.classList.contains('details-collapsed');
    }

    container.classList.toggle('details-collapsed', isCollapsed);

    if (isCollapsed) {
        btnIcon.className = 'fas fa-chevron-left';
        elements.toggleDetailsBtn.title = "展开详情";
    } else {
        btnIcon.className = 'fas fa-chevron-right';
        elements.toggleDetailsBtn.title = "收起详情";
    }

    // 触发 vis-network 重新调整大小
    if (network) {
        // 延迟一点时间等待 CSS 过渡
        setTimeout(() => network.redraw(), 350);
    }
}


// --- DOMContentLoaded ---
// [v2.5 修正] 将所有初始化代码移到 graphExplorer 对象中
const graphExplorer = {
    init() {
        // [v2.5 修正] 修复 elements 定义
        elements = {
            schemaNodeLabels: D('schema-node-labels'),
            schemaRelTypes: D('schema-rel-types'),
            searchInput: D('graph-search'),
            searchResultsContainer: D('search-results-container'), // 修正: ID 变更
            graphContainer: D('graph-container'),
            detailsPanelEl: D('graph-details-panel'),
            graphControls: D('graph-controls'), // 修正: ID 变更
            toggleDetailsBtn: D('toggle-details-btn') // [v2.5] 新增
        };

        // 检查关键元素是否存在
        if (!elements.graphContainer || !elements.schemaNodeLabels || !elements.searchInput) {
            console.error("Graph Explorer initialization failed: Missing critical elements.");
            return;
        }

        // [v2.5 新增] 初始化 UI 模块
        ui.init();

        initVisNetwork();
        loadSchemaInfo();
        clearNodeDetails(); // 默认清空
        toggleDetailsPanel(false); // 默认收起

        // 搜索框事件
        elements.searchInput.addEventListener('input', debounce(performSearch, 300));

        // 点击外部隐藏搜索结果
        document.addEventListener('click', (e) => {
            if (elements.graphControls && !elements.graphControls.contains(e.target)) {
                if (elements.searchResultsContainer) {
                    elements.searchResultsContainer.style.display = 'none';
                }
            }
        });

        // [v2.5 新增] 详情面板切换按钮事件
        elements.toggleDetailsBtn.addEventListener('click', () => toggleDetailsPanel());
    }
};

document.addEventListener('DOMContentLoaded', () => {
    graphExplorer.init();
});

// --- 模块导出 ---
// [v2.5 修正] 不再导出 graphExplorer
// export { graphExplorer };

