/**
 * 文件路径: src/main/resources/static/js/components/graphExplorer.js
 * 修改说明 v2.8:
 * 1. 修正 performSearch 函数，使其始终从 input 元素获取值，忽略传入的事件对象。
 */
import { D, debounce } from '../utils/helpers.js';
import { api } from '../services/apiService.js';
import { ui } from '../utils/ui.js';

// 模块内部变量
let network = null;
let nodes = new vis.DataSet([]);
let edges = new vis.DataSet([]);
let elements = {}; // UI 元素缓存
const EXPANDABLE_NODE_COLOR = { border: '#ff7700', highlight: { border: '#ff7700'}, hover: { border: '#ff7700'} }; // 可展开节点的颜色

/**
 * 初始化 vis-network 画布
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
            borderWidthSelected: 4, // 选中时边框加粗
            color: { // 默认颜色
                border: '#667eea',
                background: '#D2E5FF',
                highlight: {
                    border: '#667eea',
                    background: '#E3E8FF'
                },
                hover: {
                    border: '#667eea',
                    background: '#E3E8FF'
                }
            }
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
                strokeWidth: 0,
                background: 'rgba(255,255,255,0.7)'
            },
            smooth: {
                type: 'dynamic'
            }
        },
        interaction: {
            hover: true,
            tooltipDelay: 200,
            navigationButtons: false, // **修正: 禁用默认导航按钮**
            keyboard: true
        },
        physics: {
            enabled: true,
            solver: 'forceAtlas2Based',
            forceAtlas2Based: {
                gravitationalConstant: -55,
                centralGravity: 0.015,
                springLength: 120,
                springConstant: 0.08,
                avoidOverlap: 0.6
            },
            minVelocity: 0.75,
            stabilization: {
                iterations: 200
            }
        },
        groups: {
            expandable: {
                color: EXPANDABLE_NODE_COLOR,
                borderWidth: 3
            }
        }
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

    network.on("stabilizationIterationsDone", () => {
        network.fit();
    });
}

/**
 * 加载图谱 Schema（节点标签、关系类型）
 */
async function loadSchemaInfo() {
    try {
        ui.showLoading();
        const schema = await api.getGraphSchema();
        renderSchemaInfo(schema.labels || [], elements.schemaNodeLabels, 'label');
        renderSchemaInfo(schema.relationshipTypes || [], elements.schemaRelTypes, 'relationship');
    } catch (error) {
        console.error("Failed to load graph schema:", error);
        if (elements.schemaNodeLabels) {
            elements.schemaNodeLabels.innerHTML = '<p class="schema-error">加载标签失败</p>';
        }
        if (elements.schemaRelTypes) {
            elements.schemaRelTypes.innerHTML = '<p class="schema-error">加载关系失败</p>';
        }
    } finally {
        ui.hideLoading();
    }
}

/**
 * 渲染 Schema 标签
 */
function renderSchemaInfo(items, container, type) {
    if (!container) return;
    container.innerHTML = '';
    if (items && items.length > 0) {
        items.forEach(item => {
            const el = document.createElement('div');
            el.className = 'schema-tag';
            // 显示名称和数量，如果数量无效则不显示
            el.innerHTML = `<span>${item.name}</span> ${item.count >= 0 ? `<span class="schema-count">${item.count}</span>` : ''}`;
            el.title = `点击加载 "${item.name}" 节点`;

            if (type === 'label') {
                el.addEventListener('click', async () => {
                    // 点击标签时加载初始节点
                    clearNodeDetails(); // 清除详情
                    nodes.clear();      // 清空现有节点
                    edges.clear();      // 清空现有关系
                    ui.showLoading();
                    try {
                        const initialData = await api.getGraphNodesByLabel(item.name, 5); // 调用新API
                        renderGraph(initialData, true); // 使用 clear=true 进行渲染
                    } catch (error) {
                        console.error(`Failed to load initial nodes for label ${item.name}:`, error);
                        ui.showToast(`加载 ${item.name} 节点失败`, 'error');
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
 * **修正:** 不再接收 searchTerm 参数，直接从 input 读取
 * @returns {Promise<Array>} 搜索结果数组
 */
async function performSearch() {
    // **修正:** 直接从 input 元素获取值
    const term = elements.searchInput.value.trim();

    if (!term) {
        if (elements.searchResultsContainer) {
            elements.searchResultsContainer.style.display = 'none';
        }
        return [];
    }

    // 显示加载指示
    if (elements.searchResultsContainer) {
        elements.searchResultsContainer.innerHTML = '<div class="search-result-item">正在搜索...</div>';
        elements.searchResultsContainer.style.display = 'block';
    }

    try {
        const results = await api.searchGraphNodes(term); // 使用获取到的 term
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
            const nodeInfo = node.labels ? node.labels.join(', ') : '无标签';

            item.innerHTML = `<span style="font-weight: bold;">${nodeLabel}</span><br><span class="search-result-label">${nodeInfo}</span>`;
            item.title = `ID: ${node.id}`; // 悬停显示 ID

            item.addEventListener('click', () => {
                // 点击搜索结果加载并居中该节点，不清空现有图形
                loadAndRenderNodeById(node.id, nodeLabel, node, false);
            });
            elements.searchResultsContainer.appendChild(item);
        });
    }
    elements.searchResultsContainer.style.display = 'block';
}


/**
 * 辅助函数，决定节点上显示什么
 * 优先显示中文 '名称', 'name', 'label' 属性，最后回退到第一个标签或ID
 */
function getNodeDisplayLabel(nodeData) {
    if (!nodeData) return "N/A";
    const props = nodeData.properties || {};

    if (props["名称"]) return String(props["名称"]);
    if (props.name) return String(props.name);
    if (props.label) return String(props.label);

    if (nodeData.labels && nodeData.labels.length > 0) {
        const specificLabel = nodeData.labels.find(l => l !== '设备' && l !== '机组');
        if (specificLabel) return specificLabel;
        return nodeData.labels[0];
    }
    return String(nodeData.id);
}

/**
 * 加载一个节点并将其添加到图谱中
 * @param {number|string} nodeId 节点ID
 * @param {string} nodeLabel 节点显示名称（来自搜索结果）
 * @param {object} nodeData 完整的节点数据
 * @param {boolean} clearGraph 是否在添加前清空图形，默认为 false
 */
function loadAndRenderNodeById(nodeId, nodeLabel, nodeData, clearGraph = false) {
    if (elements.searchResultsContainer) {
        elements.searchResultsContainer.style.display = 'none';
    }
    if (elements.searchInput) {
        elements.searchInput.value = ''; // 清空搜索框
    }

    if (clearGraph) {
        nodes.clear();
        edges.clear();
    }

    // 检查节点是否已存在
    const existingNode = nodes.get(nodeId);
    let nodeToRender = null;

    if (!existingNode) {
        nodeToRender = {
            id: nodeId,
            label: nodeLabel || String(nodeId),
            title: formatNodeTitle(nodeData), // 悬停提示
            properties: nodeData.properties,
            labels: nodeData.labels,
            // 初始加载时不知道是否可展开，先不设置 group
            // group: nodeData.hasMoreRelationships ? 'expandable' : undefined
        };
        nodes.add(nodeToRender);
        console.log("Added node:", nodeId);
    } else {
        nodeToRender = existingNode; // 节点已存在
        console.log("Node already exists:", nodeId);
    }

    showNodeDetails(nodeToRender); // 显示选中节点的详情

    // 聚焦到该节点
    if (network) {
        network.focus(nodeId, { scale: 1.0, animation: true }); // 聚焦并适度缩放
        // 可以考虑选中节点以突出显示
        network.selectNodes([nodeId]);
    }

    // 搜索结果点击后，通常需要立即展开
    expandNode(nodeId);
}


/**
 * 展开一个节点（加载其邻居和关系）
 * @param {number|string} nodeId
 */
async function expandNode(nodeId) {
    if (!network) return; // 确保 network 已初始化

    const clickedNode = nodes.get(nodeId);
    if (!clickedNode) {
        console.warn("Cannot expand node: Node not found in dataset", nodeId);
        return;
    }

    // 可选：添加视觉反馈，表示正在加载
    // 使用 update 方法临时改变节点样式，而不是 clustering.updateClusteredNode
    try {
        nodes.update({ id: nodeId, shape: 'icon', icon: { code: '\uf110', color: '#667eea', size: clickedNode.size || 20 } });
    } catch(e) { console.warn("Failed to update node shape for loading indicator", e); }


    try {
        ui.showLoading(); // 全局加载指示器
        const data = await api.expandGraphNode(nodeId);

        if (data && data.nodes && data.edges) {
            renderGraph(data, false); // clear=false, 追加数据
        } else {
            console.warn("expandNode API 返回的数据格式不正确: ", data);
        }
    } catch (error) {
        console.error("Failed to expand node:", error);
        ui.showToast(`展开节点 ${nodeId} 失败`, 'error');
    } finally {
        ui.hideLoading();
        // 恢复节点原始样式
        const finalNode = nodes.get(nodeId); // 重新获取可能已更新的节点
        if (finalNode) {
            try {
                nodes.update({ id: nodeId, shape: 'dot' }); // 恢复为点
            } catch(e) { console.warn("Failed to restore node shape after expansion", e); }
        }
    }
}


/**
 * 渲染图谱数据（节点和关系）
 * @param {object} data - 包含 nodes 和 edges 数组的数据
 * @param {boolean} clear - 是否在渲染前清空画布
 */
function renderGraph(data, clear = false) {
    if (!network) return;

    if (clear) {
        nodes.clear();
        edges.clear();
    }

    const newNodes = [];
    const updatedNodes = []; // 用于更新已有节点（例如添加展开标记）

    if (data.nodes && data.nodes.length > 0) {
        data.nodes.forEach(node => {
            const existingNode = nodes.get(node.id);
            const nodeLabel = getNodeDisplayLabel(node);
            const nodeGroup = node.hasMoreRelationships ? 'expandable' : undefined; // 根据标志设置组

            if (!existingNode) {
                newNodes.push({
                    id: node.id,
                    label: nodeLabel,
                    title: formatNodeTitle(node),
                    properties: node.properties,
                    labels: node.labels,
                    group: nodeGroup // 设置组
                });
            } else {
                // 如果节点已存在，检查是否需要更新其可展开状态
                if (existingNode.group !== nodeGroup) {
                    updatedNodes.push({
                        id: node.id,
                        group: nodeGroup,
                        // 如果需要，也可以更新 label 或 title
                        // label: nodeLabel,
                        // title: formatNodeTitle(node),
                    });
                }
            }
        });
    }

    if (newNodes.length > 0) {
        nodes.add(newNodes);
        console.log("Added new nodes:", newNodes.length);
    }
    if (updatedNodes.length > 0) {
        nodes.update(updatedNodes);
        console.log("Updated existing nodes:", updatedNodes.length);
    }

    const newEdges = [];
    if (data.edges && data.edges.length > 0) {
        data.edges.forEach(edge => {
            if (!edges.get(edge.id)) {
                newEdges.push({
                    id: edge.id,
                    from: edge.startNodeId,
                    to: edge.endNodeId,
                    label: edge.type,
                    title: formatEdgeTitle(edge) // 添加边的悬停提示
                });
            }
        });
    }

    if (newEdges.length > 0) {
        edges.add(newEdges);
        console.log("Added new edges:", newEdges.length);
    }

    if (newNodes.length > 0 || newEdges.length > 0) {
        // 只有在添加了新元素时才重新稳定化和缩放
        network.stabilize(1000); // 增加稳定时间
        // network.fit({ animation: { duration: 500, easingFunction: 'easeInOutQuad' } }); // 平滑缩放
    } else if (clear) {
        // 如果是清空操作但没有新数据，确保画布是空的
        nodes.clear();
        edges.clear();
    } else if (updatedNodes.length > 0) {
        // 如果只是更新了节点（比如展开状态），不需要重新稳定化，但可能需要重绘
        network.redraw();
    }

    console.log("Graph rendered. Nodes:", nodes.length, "Edges:", edges.length);
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
    // 添加属性到 title
    if(node.properties){
        for (const key in node.properties) {
            // 只显示部分关键属性或限制数量避免过长
            if (['名称', '位置编号', '资产编号', 'name', 'id', 'locationCode'].includes(key)) {
                title += `<br>${key}: ${node.properties[key]}`;
            }
        }
    }
    return title;
}

/**
 * NEW: 格式化边悬停时的提示信息
 */
function formatEdgeTitle(edge) {
    if (!edge) return '';
    let title = `<b>类型: ${edge.type}</b><br>`;
    title += `ID: ${edge.id}<br>`;
    title += `From: ${edge.startNodeId}<br>`;
    title += `To: ${edge.endNodeId}`;
    // 可选：添加边的属性
    if (edge.properties && Object.keys(edge.properties).length > 0) {
        title += '<br>--- 属性 ---';
        for (const key in edge.properties) {
            title += `<br>${key}: ${edge.properties[key]}`;
        }
    }
    return title;
}


/**
 * 在右侧面板显示节点详情
 */
function showNodeDetails(node) {
    if (!elements.detailsPanelEl || !node) return; // 检查 node 是否有效

    toggleDetailsPanel(true);

    // 确保使用 node 对象中的数据，而不是 vis-network 可能修改过的数据
    const nodeId = node.id;
    const nodeData = nodes.get(nodeId); // 从 DataSet 获取原始数据
    if (!nodeData) {
        console.warn("Could not find node data in DataSet for ID:", nodeId);
        clearNodeDetails();
        return;
    }

    const nodeLabel = nodeData.label || getNodeDisplayLabel(nodeData); // 使用 DataSet 中的 label
    const nodeLabels = nodeData.labels || [];
    const props = nodeData.properties || {};

    let contentHtml = `
        <div class="detail-item">
            <strong>名称</strong>
            <span>${nodeLabel || 'N/A'}</span>
        </div>
        <div class="detail-item">
            <strong>Neo4j ID</strong>
            <span>${nodeId}</span>
        </div>
        <div class="detail-item detail-labels">
            <strong>类型 (标签)</strong>
            ${(nodeLabels).map(l => `<span class="label-tag">${l}</span>`).join('') || '<span>无</span>'}
        </div>
    `;

    contentHtml += '<strong>属性</strong>';
    const propKeys = Object.keys(props);

    if (propKeys.length === 0) {
        contentHtml += '<p>无属性</p>';
    } else {
        // 创建表格显示属性
        contentHtml += '<table class="property-table">';
        // 标题行 （可选）
        // contentHtml += '<tr><td colspan="2" class="property-table-header">属性详情</td></tr>';
        propKeys.sort().forEach(key => { // 按字母顺序排序属性
            contentHtml += `
                <tr>
                    <td>${key}</td>
                    <td>${props[key] === null || props[key] === undefined ? '<em>null</em>' : String(props[key])}</td>
                </tr>
            `;
        });
        contentHtml += '</table>';
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
    elements.detailsPanelEl.querySelector('.details-content-body').innerHTML = '<p>点击图中的节点查看其详细信息。<br>双击节点可以展开其关联的下一层节点和关系。</p>';
    // 如果详情面板是打开的，不清空时关闭它
    // toggleDetailsPanel(false);
}

/**
 * 切换右侧详情面板的显示
 */
function toggleDetailsPanel(show) {
    const container = D('graph-explorer-container');
    if (!container || !elements.toggleDetailsBtn) return; // 检查元素是否存在
    const btnIcon = elements.toggleDetailsBtn.querySelector('i');

    let isCollapsed;

    if (typeof show === 'boolean') {
        isCollapsed = !show;
    } else {
        isCollapsed = !container.classList.contains('details-collapsed');
    }

    container.classList.toggle('details-collapsed', isCollapsed);

    if (btnIcon) {
        if (isCollapsed) {
            btnIcon.className = 'fas fa-chevron-left';
            elements.toggleDetailsBtn.title = "展开详情";
        } else {
            btnIcon.className = 'fas fa-chevron-right';
            elements.toggleDetailsBtn.title = "收起详情";
        }
    }

    // 触发 vis-network 重新调整大小
    if (network) {
        setTimeout(() => {
            if (network) { // 再次检查 network 是否存在
                network.redraw();
                network.fit({ animation: false }); // 重新适应画布大小，无动画
            }
        }, 350); // 等待 CSS 过渡
    }
}


// --- DOMContentLoaded ---
const graphExplorer = {
    init() {
        elements = {
            schemaNodeLabels: D('schema-node-labels'),
            schemaRelTypes: D('schema-rel-types'),
            searchInput: D('graph-search'),
            searchResultsContainer: D('search-results-container'),
            graphContainer: D('graph-container'),
            detailsPanelEl: D('graph-details-panel'),
            graphControls: D('graph-controls'),
            toggleDetailsBtn: D('toggle-details-btn')
        };

        if (!elements.graphContainer || !elements.schemaNodeLabels || !elements.searchInput || !elements.detailsPanelEl) {
            console.error("Graph Explorer initialization failed: Missing critical elements.");
            // 可以考虑在此处显示错误信息给用户
            if(document.body) {
                document.body.innerHTML = '<p style="color:red; padding: 20px;">知识探索组件初始化失败，缺少必要的HTML元素。</p>';
            }
            return;
        }

        // 更新搜索框提示
        elements.searchInput.placeholder = "搜索节点名称、ID、位置编码或 属性名:属性值 ...";

        ui.init();
        initVisNetwork();
        loadSchemaInfo();
        clearNodeDetails();
        toggleDetailsPanel(false); // 默认收起

        // **修正:** debounce 的回调不应接收参数，performSearch 会自己读取 input 值
        elements.searchInput.addEventListener('input', debounce(() => performSearch(), 400));


        document.addEventListener('click', (e) => {
            if (elements.graphControls && !elements.graphControls.contains(e.target) && elements.searchResultsContainer) {
                elements.searchResultsContainer.style.display = 'none';
            }
        });

        if (elements.toggleDetailsBtn) {
            elements.toggleDetailsBtn.addEventListener('click', () => toggleDetailsPanel());
        }
    }
};

document.addEventListener('DOMContentLoaded', () => {
    graphExplorer.init();
});

