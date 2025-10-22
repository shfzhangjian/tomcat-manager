package com.zhangjian.tomcatmanager.dto;

import java.util.List;

public class DatabaseObject {
    private String name;
    private String type; // e.g., TABLE, VIEW
    private String comment; // Object comments
    private List<DatabaseObject> children;

    // New constructor to fix the build error
    public DatabaseObject(String name, String type, String comment) {
        this(name, type, comment, null);
    }

    public DatabaseObject(String name, String type, String comment, List<DatabaseObject> children) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.children = children;
    }

    // Getters and Setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getComment() { return comment; }
    public void setComment(String comment) { this.comment = comment; }
    public List<DatabaseObject> getChildren() { return children; }
    public void setChildren(List<DatabaseObject> children) { this.children = children; }
}

