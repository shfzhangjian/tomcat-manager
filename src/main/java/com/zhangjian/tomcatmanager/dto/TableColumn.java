package com.zhangjian.tomcatmanager.dto;

public class TableColumn {
    private String name;
    private String type;
    private int size;
    private String comment;

    public TableColumn(String name, String type, int size, String comment) {
        this.name = name;
        this.type = type;
        this.size = size;
        this.comment = comment;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}

