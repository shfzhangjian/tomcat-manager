package com.zhangjian.tomcatmanager.sync.model;

import java.util.List;
import java.util.Map;

public class NodeMapping {
    private String label;
    private String source; // table name
    private String primaryKey;
    private Map<String, String> properties; // neo4j_prop -> oracle_column

    // Getters and Setters
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
