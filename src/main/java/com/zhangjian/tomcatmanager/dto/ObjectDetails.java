package com.zhangjian.tomcatmanager.dto;

import java.util.List;

public class ObjectDetails {
    private String name;
    private String type;
    private String ddl;
    private List<TableColumn> columns;
    private String tableComment; // 新增表注释
    private List<RelatedObjectGroup> relatedObjectGroups; // 从 relatedObjects 更改

    // Getters and Setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getDdl() { return ddl; }
    public void setDdl(String ddl) { this.ddl = ddl; }
    public List<TableColumn> getColumns() { return columns; }
    public void setColumns(List<TableColumn> columns) { this.columns = columns; }
    public String getTableComment() { return tableComment; }
    public void setTableComment(String tableComment) { this.tableComment = tableComment; }
    public List<RelatedObjectGroup> getRelatedObjectGroups() { return relatedObjectGroups; }
    public void setRelatedObjectGroups(List<RelatedObjectGroup> relatedObjectGroups) { this.relatedObjectGroups = relatedObjectGroups; }
}
