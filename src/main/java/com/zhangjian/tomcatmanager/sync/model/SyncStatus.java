package com.zhangjian.tomcatmanager.sync.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents the current status and history of a synchronization task for a specific database connection.
 */
public class SyncStatus {
    /**
     * Current status of the sync task (e.g., UNKNOWN, IN_PROGRESS, SUCCESS, FAIL).
     */
    private String status = "UNKNOWN";

    /**
     * Timestamp of the last time a synchronization attempt finished (successfully or not).
     */
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastSyncTime;

    /**
     * Duration of the last synchronization attempt in milliseconds.
     */
    private Long lastSyncDurationMs;

    /**
     * A list containing the history of recent synchronization attempts. Limited by MAX_HISTORY.
     */
    private final List<SyncHistory> history = new LinkedList<>(); // Use LinkedList for efficient addition
    private static final int MAX_HISTORY = 20; // Keep only the last 20 history entries

    // --- Getters and Setters ---
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public LocalDateTime getLastSyncTime() { return lastSyncTime; }
    public void setLastSyncTime(LocalDateTime lastSyncTime) { this.lastSyncTime = lastSyncTime; }
    public Long getLastSyncDurationMs() { return lastSyncDurationMs; }
    public void setLastSyncDurationMs(Long lastSyncDurationMs) { this.lastSyncDurationMs = lastSyncDurationMs; }
    public List<SyncHistory> getHistory() { return history; }

    /**
     * Adds a new history entry to the beginning of the list, maintaining the maximum history size.
     * This method is synchronized to ensure thread safety when updating the history list.
     * @param entry The SyncHistory entry to add.
     */
    public void addHistory(SyncHistory entry) {
        synchronized (history) {
            history.add(0, entry); // Add to the beginning for chronological order (newest first)
            // Ensure the history list does not exceed the maximum size
            if (history.size() > MAX_HISTORY) {
                history.remove(history.size() - 1); // Remove the oldest entry from the end
            }
        }
    }

    /**
     * Inner class representing a single entry in the synchronization history.
     */
    public static class SyncHistory {
        /**
         * Timestamp when this history event occurred.
         */
        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private final LocalDateTime timestamp;
        /**
         * Status of the sync task at the time of this entry (e.g., SUCCESS, FAIL).
         */
        private final String status;
        /**
         * A brief message describing the event or outcome.
         */
        private final String message;
        /**
         * Duration of the sync task for this entry, if applicable.
         */
        private final Long durationMs;

        /**
         * Constructor for SyncHistory.
         * @param timestamp The time the event occurred.
         * @param status The status code (e.g., SUCCESS, FAIL).
         * @param message A descriptive message.
         * @param durationMs The duration in milliseconds, if applicable.
         */
        public SyncHistory(LocalDateTime timestamp, String status, String message, Long durationMs) {
            this.timestamp = timestamp;
            this.status = status;
            // Truncate long messages to prevent overly large JSON payloads
            this.message = (message != null && message.length() > 200) ? message.substring(0, 197) + "..." : message;
            this.durationMs = durationMs;
        }

        // --- Getters ---
        public LocalDateTime getTimestamp() { return timestamp; }
        public String getStatus() { return status; }
        public String getMessage() { return message; }
        public Long getDurationMs() { return durationMs; }
    }
}

