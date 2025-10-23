package com.zhangjian.tomcatmanager.sync.model;

import java.util.List;
import java.util.Map;

public class MappingConfig {
    private Map<String, NodeMapping> nodes;
    private List<RelationshipMapping> relationships;

    // Getters and Setters
    public Map<String, NodeMapping> getNodes() {
        return nodes;
    }

    public void setNodes(Map<String, NodeMapping> nodes) {
        this.nodes = nodes;
    }

    public List<RelationshipMapping> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<RelationshipMapping> relationships) {
        this.relationships = relationships;
    }
}
