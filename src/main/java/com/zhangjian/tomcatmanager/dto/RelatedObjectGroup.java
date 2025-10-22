package com.zhangjian.tomcatmanager.dto;

import java.util.List;

public class RelatedObjectGroup {
    private String type; // 例如: "外键", "依赖视图"
    private List<DatabaseObject> objects;

    public RelatedObjectGroup(String type, List<DatabaseObject> objects) {
        this.type = type;
        this.objects = objects;
    }

    // Getters and Setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public List<DatabaseObject> getObjects() { return objects; }
    public void setObjects(List<DatabaseObject> objects) { this.objects = objects; }
}
