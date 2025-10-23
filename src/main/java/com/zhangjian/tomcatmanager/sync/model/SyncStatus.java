package com.zhangjian.tomcatmanager.sync.model;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

public class SyncStatus {
    private String status = "IDLE"; // e.g., IDLE, IN_PROGRESS, SUCCESS, FAIL
    private LocalDateTime lastSyncTime;
    private Long lastSyncDurationMs;
    private final LinkedList<SyncHistory> history = new LinkedList<>();
    private static final int MAX_HISTORY = 20;

    public static class SyncHistory {
        private final LocalDateTime timestamp;
        private final String status;
        private final String message;
        private final Long durationMs;

        public SyncHistory(String status, String message, Long durationMs) {
            this.timestamp = LocalDateTime.now();
            this.status = status;
            this.message = message;
            this.durationMs = durationMs;
        }

        // Getters are needed for JSON serialization
        public LocalDateTime getTimestamp() { return timestamp; }
        public String getStatus() { return status; }
        public String getMessage() { return message; }
        public Long getDurationMs() { return durationMs; }
    }

    public void addHistory(SyncHistory historyEntry) {
        history.addFirst(historyEntry);
        if (history.size() > MAX_HISTORY) {
            history.removeLast();
        }
    }

    // Getters and Setters
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public LocalDateTime getLastSyncTime() { return lastSyncTime; }
    public void setLastSyncTime(LocalDateTime lastSyncTime) { this.lastSyncTime = lastSyncTime; }
    public Long getLastSyncDurationMs() { return lastSyncDurationMs; }
    public void setLastSyncDurationMs(Long lastSyncDurationMs) { this.lastSyncDurationMs = lastSyncDurationMs; }
    public List<SyncHistory> getHistory() { return history; }
}

