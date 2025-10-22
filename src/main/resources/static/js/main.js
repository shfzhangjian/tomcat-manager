import { api } from './services/apiService.js';
import { ui } from './utils/ui.js';
import { connectionManager } from './components/connectionManager.js';
import { workspaceManager } from './components/workspaceManager.js';
import { detailsPanel } from './components/detailsPanel.js';

document.addEventListener('DOMContentLoaded', () => {
    // 1. Initialize UI utilities first
    ui.init();

    // 2. Initialize the main components
    detailsPanel.init(document.getElementById('detailsPanel'));
    workspaceManager.init(
        document.getElementById('centerPanel'),
        // Callbacks from workspace components to other components
        {
            onViewDetails: (connId, type, name) => {
                ui.toggleDetailsPanel(true);
                detailsPanel.load(connId, type, name);
            },
            onTrainAi: (connId, type, name) => {
                api.getObjectDetails(connId, type, name).then(details => {
                    ui.showAiPromptModal(details);
                });
            }
        }
    );

    connectionManager.init({
        onSelectConnection: (connId, connName) => {
            workspaceManager.render(connId, connName);
            ui.toggleDetailsPanel(false); // Hide details panel when switching connections
        }
    });

    // 3. Load initial data
    connectionManager.load();

    // **【修正点 B】**: 添加缺失的侧边栏控制按钮的事件监听器
    if (ui.elements.sidebarCollapseBtn) {
        ui.elements.sidebarCollapseBtn.addEventListener('click', () => ui.toggleSidebar(true));
    }
    if (ui.elements.leftPanelExpandBtn) {
        ui.elements.leftPanelExpandBtn.addEventListener('click', () => ui.toggleSidebar(false));
    }
    if (ui.elements.rightPanelToggleBtn) {
        ui.elements.rightPanelToggleBtn.addEventListener('click', () => ui.toggleDetailsPanel());
    }
});