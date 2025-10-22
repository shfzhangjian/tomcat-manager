import { D } from '../utils/helpers.js';

const api = {
    /**
     * A generic fetch wrapper for API calls.
     * @param {string} endpoint - The API endpoint to call.
     * @param {Object} options - The options for the fetch call.
     * @param {boolean} [isBlob=false] - Whether to expect a Blob response.
     * @returns {Promise<any>} The JSON or Blob response.
     */
    async apiCall(endpoint, options, isBlob = false) {
        try {
            const response = await fetch(`/api/${endpoint}`, options);
            if (!response.ok) {
                if (response.status === 404 && endpoint.includes('/layouts/')) {
                    console.log("Layout not found, using defaults.");
                    return null;
                }
                const errorData = await response.json();
                throw new Error(errorData.message || '操作失败');
            }
            if (isBlob) return await response.blob();
            const text = await response.text();
            return text ? JSON.parse(text) : null;
        } catch (error) {
            // Avoid showing toast for non-critical errors like layout not found
            if (!endpoint.includes('/layouts/')) {
                ui.showToast(error.message, 'error');
            }
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
    getLayoutsForSql: (sqlHash) => api.apiCall(`db/layouts/${sqlHash}`, { method: 'GET' }), // FIX: Changed 'layout' to 'layouts'
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
    })
};

// Make it globally accessible or export it
window.api = api; // For simplicity in this single-page app context
export { api };

