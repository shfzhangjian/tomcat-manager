/**
 * 文件路径: src/main/resources/static/js/services/apiService.js
 * 修改说明 v2.6:
 * 1. 新增 getGraphNodesByLabel 方法。
 * 2. searchGraphNodes 无需修改，由 GraphService 处理 term 格式。
 * 3. 修正 saveSyncMapping 错别字 (saveMappingConfig -> saveMappingConfig)。
 */
import { D } from '../utils/helpers.js';

const ui = {
    showToast: (message, type) => console.log(`Toast (${type}): ${message}`),
};

const api = {
    /**
     * A generic fetch wrapper for API calls.
     * @param {string} endpoint - The API endpoint to call.
     * @param {Object} options - The options for the fetch call.
     * @param {boolean} [isBlob=false] - Whether to expect a Blob response.
     * @param {boolean} [isText=false] - Whether to expect a plain text response.
     * @returns {Promise<any>} The JSON, Blob, or text response.
     */
    async apiCall(endpoint, options, isBlob = false, isText = false) {
        try {
            const response = await fetch(`/api/${endpoint}`, options);
            if (!response.ok) {
                if (response.status === 404 && endpoint.includes('/layouts/')) {
                    console.log("Layout not found, using defaults.");
                    return null;
                }
                const errorData = await response.json().catch(() => ({ message: `HTTP error! status: ${response.status}` }));
                throw new Error(errorData.message || '操作失败');
            }
            if (isBlob) return await response.blob();

            const text = await response.text();

            // If a plain text response is expected, return it directly
            if(isText) {
                return text;
            }

            // Otherwise, parse as JSON (default behavior)
            // Handle potentially empty successful responses (e.g., DELETE)
            if (!text) {
                return null;
            }
            try {
                return JSON.parse(text);
            } catch (jsonError) {
                console.error("Failed to parse JSON response:", text, jsonError);
                throw new Error("Received invalid JSON response from server.");
            }
        } catch (error) {
            // Re-throw the error to be handled by the caller's catch block
            throw error;
        }
    },


    // --- Connection Management ---
    getConnections: () => api.apiCall('db/connections', { method: 'GET' }),
    saveConnection: (connData) => api.apiCall('db/connections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(connData)
    }),
    deleteConnection: (id) => api.apiCall(`db/connections/${id}`, { method: 'DELETE' }),
    testConnection: (connData) => api.apiCall('db/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(connData)
    }),
    toggleSync: (connId, enabled) => api.apiCall(`sync/toggle/${connId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: enabled })
    }),

    // --- Object Browser ---
    getObjectTypes: (connId) => api.apiCall(`db/connections/${connId}/object-types`, { method: 'GET' }),
    getObjects: (connId, type, page, filter) => {
        const url = `db/connections/${connId}/objects/${type}?page=${page}&size=50` + (filter ? `&filter=${encodeURIComponent(filter)}` : '');
        return api.apiCall(url, { method: 'GET' });
    },
    getObjectDetails: (connId, type, name) => api.apiCall(`db/connections/${connId}/objects/${type}/${name}/details`, { method: 'GET' }),

    // --- SQL Editor ---
    executeQuery: (connId, sql, page, size) => api.apiCall(`db/connections/${connId}/query?page=${page}&size=${size}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql })
    }),
    exportToExcel: (connId, sql) => api.apiCall(`db/connections/${connId}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql })
    }, true),

    // --- Layout Management ---
    getLayoutsForSql: (sqlHash) => api.apiCall(`db/layouts/${sqlHash}`, { method: 'GET' }),
    saveLayout: (layoutData) => api.apiCall('db/layout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(layoutData)
    }),
    deleteLayout: (layoutId) => api.apiCall(`db/layout/${layoutId}`, { method: 'DELETE' }),

    // --- AI ---
    getAiPrompts: () => api.apiCall('ai/prompts', { method: 'GET' }),
    saveAiPrompt: (promptData) => api.apiCall('ai/prompts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(promptData)
    }),
    queryAi: (query, context) => api.apiCall('ai/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, context })
    }),

    // --- Graph Explorer ---
    searchGraphNodes: (term) => api.apiCall(`graph/search?term=${encodeURIComponent(term)}`, { method: 'GET' }),
    expandGraphNode: (nodeId) => api.apiCall(`graph/expand?nodeId=${encodeURIComponent(nodeId)}`, { method: 'GET' }),
    getGraphSchema: () => api.apiCall('graph/schema', { method: 'GET' }),
    /**
     * NEW: Gets initial nodes and relationships by label.
     * @param {string} label The node label.
     * @param {number} limit The maximum number of nodes.
     * @returns {Promise<object>} Graph data { nodes: [], edges: [] }.
     */
    getGraphNodesByLabel: (label, limit) => api.apiCall(`graph/nodes-by-label?label=${encodeURIComponent(label)}&limit=${limit}`, { method: 'GET' }),

    // --- Sync ---
    getMappingConfig: (connId) => api.apiCall(`sync/mapping/${connId}`, { method: 'GET' }, false, true), // Expect a text response
    // 修正: 接口名称统一为 saveMappingConfig
    saveMappingConfig: (connId, config) => api.apiCall(`sync/mapping/${connId}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/yaml' }, // Correct Content-Type for YAML
        body: config
    }),
    triggerSync: (connId, isDelta = false) => api.apiCall(`sync/trigger/${connId}?delta=${isDelta}`, { method: 'POST' }),
    getSyncStatus: (connId) => api.apiCall(`sync/status/${connId}`, { method: 'GET' }),
};

// Make it globally accessible or export it
window.api = api; // For simplicity in this single-page app context
export { api };
