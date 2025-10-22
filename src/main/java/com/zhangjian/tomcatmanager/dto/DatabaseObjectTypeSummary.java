package com.zhangjian.tomcatmanager.dto;

public class DatabaseObjectTypeSummary {
    private String type; // e.g., TABLE
    private String displayName; // e.g., Tables
    private long count;

    public DatabaseObjectTypeSummary(String type, String displayName, long count) {
        this.type = type;
        this.displayName = displayName;
        this.count = count;
    }

    // Getters and Setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String displayName) { this.displayName = displayName; }
    public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
}
