import { api } from '../services/apiService.js';
import { debounce } from '../utils/helpers.js';

document.addEventListener('DOMContentLoaded', () => {

    const networkContainer = document.getElementById('graph-network');
    const searchInput = document.getElementById('graphSearchInput');

    const nodes = new vis.DataSet([]);
    const edges = new vis.DataSet([]);

    const options = {
        nodes: {
            shape: 'dot',
            size: 16,
            font: {
                size: 14,
                color: '#333'
            },
            borderWidth: 2,
        },
        edges: {
            width: 2,
            arrows: 'to',
            font: {
                align: 'middle'
            }
        },
        physics: {
            forceAtlas2Based: {
                gravitationalConstant: -26,
                centralGravity: 0.005,
                springLength: 230,
                springConstant: 0.18,
            },
            maxVelocity: 146,
            solver: "forceAtlas2Based",
            timestep: 0.35,
            stabilization: { iterations: 150 },
        },
        interaction: {
            tooltipDelay: 200,
            hideEdgesOnDrag: true,
        },
    };

    const network = new vis.Network(networkContainer, { nodes, edges }, options);

    const search = debounce(async (term) => {
        if (!term) {
            return;
        }
        try {
            const results = await api.searchGraphNodes(term);
            // Simple approach: clear and add new results
            nodes.clear();
            edges.clear();
            nodes.add(results.nodes);
        } catch (error) {
            console.error("Search failed:", error);
        }
    }, 300);

    const expandNode = async (nodeId) => {
        if(!nodeId) return;
        try {
            const result = await api.expandGraphNode(nodeId);
            nodes.update(result.nodes);
            edges.update(result.edges);
        } catch (error) {
            console.error("Expand failed:", error);
        }
    };

    searchInput.addEventListener('input', () => {
        search(searchInput.value);
    });

    network.on("doubleClick", (params) => {
        if (params.nodes.length > 0) {
            const nodeId = params.nodes[0];
            expandNode(nodeId);
        }
    });
});
