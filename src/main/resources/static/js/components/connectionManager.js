import { api } from '../services/apiService.js';
import { ui } from '../utils/ui.js';

const D = id => document.getElementById(id);

let onSelectConnectionCallback = null;
let connectionsCache = [];

function render(connections) {
    connectionsCache = connections;
    const listEl = D('connectionList');
    listEl.innerHTML = '';
    connections.forEach(conn => {
        const li = document.createElement('li');
        li.className = 'connection-item';
        li.dataset.id = conn.id;

        const infoDiv = document.createElement('div');
        infoDiv.className = 'connection-info';
        infoDiv.innerHTML = `<i class="fas fa-database"></i><span title="${conn.name}">${conn.name}</span>`;
        infoDiv.addEventListener('click', () => {
            document.querySelectorAll('.connection-item').forEach(el => el.classList.remove('active'));
            li.classList.add('active');
            if (onSelectConnectionCallback) {
                onSelectConnectionCallback(conn.id, conn.name);
            }
        });

        const actionsDiv = document.createElement('div');
        actionsDiv.className = 'connection-actions';
        const editBtn = document.createElement('button');
        editBtn.title = '编辑';
        editBtn.innerHTML = '<i class="fas fa-edit"></i>';
        editBtn.onclick = (e) => { e.stopPropagation(); ui.showConnectionModal(conn); };
        const deleteBtn = document.createElement('button');
        deleteBtn.title = '删除';
        deleteBtn.innerHTML = '<i class="fas fa-trash"></i>';
        deleteBtn.onclick = (e) => {
            e.stopPropagation();
            ui.showConfirm(`确定要删除连接 "${conn.name}" 吗？`, () => {
                api.deleteConnection(conn.id).then(() => {
                    ui.showToast('连接已删除');
                    connectionManager.load(); // Reload the list
                }).catch(err => ui.showToast(err.message, 'error'));
            });
        };

        actionsDiv.appendChild(editBtn);
        actionsDiv.appendChild(deleteBtn);
        li.appendChild(infoDiv);
        li.appendChild(actionsDiv);
        listEl.appendChild(li);
    });
}

async function handleSave() {
    const form = D('connectionForm');
    if (!form.checkValidity()) {
        ui.showAlert("请填写所有必填字段。");
        return;
    }
    const connection = {
        id: D('connectionId').value || null,
        name: D('connectionName').value,
        type: D('dbType').value,
        host: D('dbHost').value,
        port: D('dbPort').value,
        databaseName: D('dbName').value,
        username: D('dbUsername').value,
        password: D('dbPassword').value
    };
    try {
        await api.saveConnection(connection);
        ui.showToast('连接已保存！');
        connectionManager.load();
        ui.hideConnectionModal();
    } catch (error) {
        ui.showToast(error.message, 'error');
    }
}

async function handleTest() {
    const connection = {
        type: D('dbType').value,
        host: D('dbHost').value,
        port: D('dbPort').value,
        databaseName: D('dbName').value,
        username: D('dbUsername').value,
        password: D('dbPassword').value
    };
    try {
        const result = await api.testConnection(connection);
        ui.showToast(result.message, 'success');
    } catch(error) {
        ui.showToast(error.message, 'error');
    }
}

export const connectionManager = {
    init(callbacks) {
        onSelectConnectionCallback = callbacks.onSelectConnection;
        D('addConnectionBtn').addEventListener('click', () => ui.showConnectionModal());
        D('saveConnectionBtn').addEventListener('click', handleSave);
        D('testConnectionBtn').addEventListener('click', handleTest);
    },
    async load() {
        try {
            const connections = await api.getConnections();
            render(connections);
        } catch (error) {
            ui.showToast('加载连接列表失败: ' + error.message, 'error');
        }
    }
};
