package com.zhangjian.tomcatmanager.sync;

import com.zhangjian.tomcatmanager.DatabaseService;
import com.zhangjian.tomcatmanager.sync.model.SyncStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@RestController
@RequestMapping("/api/sync")
public class SyncController {

    private final EamSyncService eamSyncService;
    private final SyncLogService syncLogService;
    private final DatabaseService databaseService;

    public SyncController(EamSyncService eamSyncService, SyncLogService syncLogService, DatabaseService databaseService) {
        this.eamSyncService = eamSyncService;
        this.syncLogService = syncLogService;
        this.databaseService = databaseService;
    }

    @GetMapping("/mapping/{connectionId}")
    public ResponseEntity<String> getMapping(@PathVariable String connectionId) {
        try {
            return ResponseEntity.ok(eamSyncService.getMappingConfig(connectionId));
        } catch (IOException e) {
            return ResponseEntity.ok("# Unable to load mapping config: " + e.getMessage());
        }
    }

    @PostMapping("/mapping/{connectionId}")
    public ResponseEntity<Map<String, String>> saveMapping(@PathVariable String connectionId, @RequestBody String mappingConfig) {
        try {
            eamSyncService.saveMappingConfig(connectionId, mappingConfig);
            return ResponseEntity.ok(Collections.singletonMap("message", "Mapping configuration saved."));
        } catch (IOException e) {
            return ResponseEntity.status(500).body(Collections.singletonMap("message", "Failed to save mapping: " + e.getMessage()));
        }
    }

    @PostMapping("/trigger/{connectionId}")
    public ResponseEntity<Map<String, String>> triggerSync(@PathVariable String connectionId) {
        eamSyncService.executeSync(connectionId);
        return ResponseEntity.ok(Collections.singletonMap("message", "Sync triggered for connection " + connectionId));
    }

    @GetMapping("/status/{connectionId}")
    public ResponseEntity<SyncStatus> getStatus(@PathVariable String connectionId) {
        return ResponseEntity.ok(eamSyncService.getSyncStatus(connectionId));
    }

    @GetMapping("/logs/subscribe/{connectionId}")
    public SseEmitter subscribeToLogs(@PathVariable String connectionId) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        syncLogService.addEmitter(connectionId, emitter);
        return emitter;
    }

    @PostMapping("/toggle/{connectionId}")
    public ResponseEntity<Map<String, String>> toggleSync(
            @PathVariable String connectionId,
            @RequestBody Map<String, Boolean> payload) {
        try {
            Boolean enabled = payload.get("enabled");
            if (enabled == null) {
                return ResponseEntity.badRequest().body(Collections.singletonMap("message", "Missing 'enabled' flag in request body."));
            }
            databaseService.updateSyncEnabled(connectionId, enabled);
            String status = enabled ? "enabled" : "disabled";
            return ResponseEntity.ok(Collections.singletonMap("message", "Sync for connection has been " + status));
        } catch (IOException e) {
            return ResponseEntity.status(500).body(Collections.singletonMap("message", "Failed to update sync status: " + e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(Collections.singletonMap("message", e.getMessage()));
        }
    }
}

