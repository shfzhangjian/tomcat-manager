package com.zhangjian.tomcatmanager.dto;

public class QueryColumnInfo {
    private String name;
    private String comment;

    public QueryColumnInfo(String name, String comment) {
        this.name = name;
        this.comment = comment;
    }

    // Getters and Setters
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
