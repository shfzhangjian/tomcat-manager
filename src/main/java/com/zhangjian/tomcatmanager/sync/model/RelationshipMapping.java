package com.zhangjian.tomcatmanager.sync.model;

public class RelationshipMapping {
    private String type;
    private String fromNode;
    private String toNode;
    private String joinOn; // e.g., "from_column=to_column"

    // Getters and Setters
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
}
