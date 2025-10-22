package com.zhangjian.tomcatmanager;

import com.zhangjian.tomcatmanager.dto.*;
import org.apache.poi.ss.usermodel.Workbook;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/db")
public class DatabaseController {

    private final DatabaseService databaseService;

    public DatabaseController(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @GetMapping("/connections")
    public List<DatabaseConnection> getConnections() {
        return databaseService.getAllConnections();
    }

    @PostMapping("/connections")
    public ResponseEntity<DatabaseConnection> saveConnection(@RequestBody DatabaseConnection connection) {
        try {
            DatabaseConnection savedConnection = databaseService.saveConnection(connection);
            return ResponseEntity.ok(savedConnection);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @DeleteMapping("/connections/{id}")
    public ResponseEntity<Void> deleteConnection(@PathVariable String id) {
        try {
            databaseService.deleteConnection(id);
            return ResponseEntity.ok().build();
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/test")
    public ResponseEntity<Map<String, String>> testConnection(@RequestBody DatabaseConnection connection) {
        String result = databaseService.testConnection(connection);
        HttpStatus status = result.startsWith("连接成功") ? HttpStatus.OK : HttpStatus.BAD_REQUEST;
        return ResponseEntity.status(status).body(Collections.singletonMap("message", result));
    }

    @GetMapping("/connections/{id}/object-types")
    public ResponseEntity<List<DatabaseObjectTypeSummary>> getDatabaseObjectTypes(@PathVariable String id) {
        try {
            List<DatabaseObjectTypeSummary> objects = databaseService.getDatabaseObjectTypes(id);
            return ResponseEntity.ok(objects);
        } catch (SQLException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    @GetMapping("/connections/{id}/objects/{type}")
    public ResponseEntity<PaginatedResult<DatabaseObject>> getPaginatedObjects(
            @PathVariable String id,
            @PathVariable String type,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            @RequestParam(required = false) String filter) {
        try {
            PaginatedResult<DatabaseObject> result = databaseService.getPaginatedObjects(id, type, page, size, filter);
            return ResponseEntity.ok(result);
        } catch (SQLException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/connections/{connectionId}/objects/{objectType}/{objectName}/details")
    public ResponseEntity<ObjectDetails> getObjectDetails(
            @PathVariable String connectionId,
            @PathVariable String objectType,
            @PathVariable String objectName) {
        try {
            ObjectDetails details = databaseService.getObjectDetails(connectionId, objectType, objectName);
            return ResponseEntity.ok(details);
        } catch (SQLException e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @PostMapping("/connections/{id}/query")
    public ResponseEntity<QueryResult> executeQuery(@PathVariable String id, @RequestBody Map<String, String> payload, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "100") int size) {
        String sql = payload.get("sql");
        QueryResult result = databaseService.executePaginatedQuery(id, sql, page, size);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/connections/{id}/export")
    public ResponseEntity<Resource> exportToExcel(@PathVariable String id, @RequestBody Map<String, String> payload) {
        try {
            String sql = payload.get("sql");
            Workbook workbook = databaseService.exportQueryToExcel(id, sql);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            workbook.write(bos);
            workbook.close();

            ByteArrayResource resource = new ByteArrayResource(bos.toByteArray());

            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=query_result.xlsx")
                    .contentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
                    .body(resource);

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // --- Layout Management ---
    @GetMapping("/layouts/{sqlHash}")
    public ResponseEntity<List<QueryLayout>> getLayoutsForSql(@PathVariable String sqlHash) {
        List<QueryLayout> foundLayouts = databaseService.getLayoutsForSql(sqlHash);
        // Always return OK, even if empty, to prevent frontend 404 errors.
        return ResponseEntity.ok(foundLayouts);
    }

    @PostMapping("/layout")
    public ResponseEntity<QueryLayout> saveLayout(@RequestBody QueryLayout layout) {
        try {
            QueryLayout savedLayout = databaseService.saveLayout(layout);
            return ResponseEntity.ok(savedLayout);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @DeleteMapping("/layout/{layoutId}")
    public ResponseEntity<Void> deleteLayout(@PathVariable String layoutId) {
        try {
            databaseService.deleteLayout(layoutId);
            return ResponseEntity.ok().build();
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

