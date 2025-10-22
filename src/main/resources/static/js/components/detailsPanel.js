import { api } from '../services/apiService.js';
import { ui } from '../utils/ui.js';

const detailsPanel = {
    container: null,
    connId: null,

    init(containerElement) {
        this.container = containerElement;
    },

    async loadDetails(connId, objectType, objectName) {
        this.connId = connId;
        ui.toggleDetailsPanel(true);
        this.container.innerHTML = '<div><i class="fas fa-spinner fa-spin"></i> 加载详情...</div>';

        try {
            const details = await api.getObjectDetails(this.connId, objectType, objectName);
            this.renderDetails(details);
        } catch (e) {
            this.container.innerHTML = '<div>加载详情失败。</div>';
        }
    },

    renderDetails(details) {
        let contentHtml = `
            <div class="details-header">
                <h3>${details.name}</h3>
                <button id="closeDetailsBtn" title="关闭详情"><i class="fas fa-times"></i></button>
            </div>
            <div class="details-content">
        `;

        if (details.ddl) {
            contentHtml += `<h4>定义 (DDL)</h4><pre><code>${details.ddl}</code></pre>`;
        }
        if (details.columns && details.columns.length > 0) {
            contentHtml += `<h4>字段</h4><table><thead><tr><th>名称</th><th>类型</th><th>长度</th><th>注释</th></tr></thead><tbody>`;
            details.columns.forEach(col => {
                contentHtml += `<tr><td>${col.name}</td><td>${col.type}</td><td>${col.size}</td><td>${col.comment || ''}</td></tr>`;
            });
            contentHtml += `</tbody></table>`;
        }
        if (details.relatedObjectGroups && details.relatedObjectGroups.length > 0) {
            details.relatedObjectGroups.forEach(group => {
                if (group.objects && group.objects.length > 0) {
                    contentHtml += `<h4>${group.type}</h4><ul>`;
                    group.objects.forEach(obj => {
                        contentHtml += `<li>${obj.name} ${obj.comment || ''}</li>`;
                    });
                    contentHtml += `</ul>`;
                }
            });
        }

        contentHtml += '</div>';
        this.container.innerHTML = contentHtml;
        this.container.querySelector('#closeDetailsBtn').addEventListener('click', () => ui.toggleDetailsPanel(false));
    }
};

export { detailsPanel };

