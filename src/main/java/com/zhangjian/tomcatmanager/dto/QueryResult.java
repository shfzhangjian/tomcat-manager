package com.zhangjian.tomcatmanager.dto;

import java.util.List;
import java.util.Map;

public class QueryResult {
    private List<QueryColumnInfo> columnInfo;
    private List<Map<String, Object>> rows;
    private String error;
    private int currentPage;
    private int totalPages;
    private long totalRows;

    public static QueryResult success(List<QueryColumnInfo> columnInfo, List<Map<String, Object>> rows, int currentPage, int totalPages, long totalRows) {
        QueryResult result = new QueryResult();
        result.columnInfo = columnInfo;
        result.rows = rows;
        result.currentPage = currentPage;
        result.totalPages = totalPages;
        result.totalRows = totalRows;
        return result;
    }

    public static QueryResult error(String error) {
        QueryResult result = new QueryResult();
        result.error = error;
        return result;
    }

    // Getters and Setters
    public List<QueryColumnInfo> getColumnInfo() { return columnInfo; }
    public void setColumnInfo(List<QueryColumnInfo> columnInfo) { this.columnInfo = columnInfo; }
    public List<Map<String, Object>> getRows() { return rows; }
    public void setRows(List<Map<String, Object>> rows) { this.rows = rows; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
    public int getCurrentPage() { return currentPage; }
    public void setCurrentPage(int currentPage) { this.currentPage = currentPage; }
    public int getTotalPages() { return totalPages; }
    public void setTotalPages(int totalPages) { this.totalPages = totalPages; }
    public long getTotalRows() { return totalRows; }
    public void setTotalRows(long totalRows) { this.totalRows = totalRows; }
}
