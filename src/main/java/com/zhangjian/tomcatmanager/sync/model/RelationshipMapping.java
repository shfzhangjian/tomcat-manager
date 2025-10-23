package com.zhangjian.tomcatmanager.sync.model;

/**
 * Represents the configuration for mapping a relationship between two types of nodes.
 */
public class RelationshipMapping {

    /**
     * The type (name) of the relationship in Neo4j (e.g., "包含", "下级").
     */
    private String type;

    /**
     * The key (defined in the 'nodes' section of YAML) of the starting node mapping.
     */
    private String fromNode;

    /**
     * The key (defined in the 'nodes' section of YAML) of the ending node mapping.
     */
    private String toNode;

    /**
     * Standard join condition based on column equality between the two source tables.
     * Format: "fromTableColumn=toTableColumn".
     * Can also be a special value like "HIERARCHY:columnName".
     */
    private String joinOn;

    /**
     * NEW: Lookup-based join condition.
     * Connects nodes based on a property value from the 'fromNode' matching
     * the primary key property value of the 'toNode'.
     * Format: "fromNodeLookupPropertyName=toNodePrimaryKeyPropertyName".
     * Example: "_temp_maker_loc=位置编号"
     */
    private String joinOnLookup;


    // --- Getters and Setters ---

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFromNode() {
        return fromNode;
    }

    public void setFromNode(String fromNode) {
        this.fromNode = fromNode;
    }

    public String getToNode() {
        return toNode;
    }

    public void setToNode(String toNode) {
        this.toNode = toNode;
    }

    public String getJoinOn() {
        return joinOn;
    }

    public void setJoinOn(String joinOn) {
        this.joinOn = joinOn;
    }

    public String getJoinOnLookup() {
        return joinOnLookup;
    }

    public void setJoinOnLookup(String joinOnLookup) {
        this.joinOnLookup = joinOnLookup;
    }


    // Helper method in RelationshipMapping or directly used:
    public String getFromNodeLabelOrDefault() { return "设备"; } // Based on your hierarchyRel config
    public String getToNodeLabelOrDefault() { return "设备"; }   // Based on your hierarchyRel config

}

