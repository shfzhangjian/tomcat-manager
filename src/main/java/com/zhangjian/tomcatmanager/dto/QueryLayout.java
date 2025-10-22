package com.zhangjian.tomcatmanager.dto;

import java.util.List;

public class QueryLayout {
    private String id; // Unique ID for the layout
    private String name; // User-defined name for the layout, null for auto-saved
    private String sqlHash;
    private List<ColumnLayout> columns;

    public static class ColumnLayout {
        private String field;
        private Object width;
        private boolean visible;

        // Getters and Setters
        public String getField() { return field; }
        public void setField(String field) { this.field = field; }
        public Object getWidth() { return width; }
        public void setWidth(Object width) { this.width = width; }
        public boolean isVisible() { return visible; }
        public void setVisible(boolean visible) { this.visible = visible; }
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getSqlHash() { return sqlHash; }
    public void setSqlHash(String sqlHash) { this.sqlHash = sqlHash; }
    public List<ColumnLayout> getColumns() { return columns; }
    public void setColumns(List<ColumnLayout> columns) { this.columns = columns; }
}

