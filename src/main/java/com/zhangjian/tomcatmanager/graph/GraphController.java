/**
 * 文件路径: src/main/java/com/zhangjian/tomcatmanager/graph/GraphController.java
 * 修改说明 v2.6:
 * 1. 新增 /nodes-by-label 端点用于按标签加载初始节点。
 * 2. searchNodes 端点现在直接接收 term 参数，由 Service 层处理属性搜索逻辑。
 */
package com.zhangjian.tomcatmanager.graph;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST Controller for graph exploration functionalities.
 */
@RestController
@RequestMapping("/api/graph")
public class GraphController {

    private final GraphService graphService;
    private static final int DEFAULT_LABEL_LOAD_LIMIT = 5; // 和 Service 保持一致

    // Constructor injection
    public GraphController(GraphService graphService) {
        this.graphService = graphService;
    }

    /**
     * Searches nodes based on a term (general or property:value).
     * @param term Search term.
     * @return List of matching nodes.
     */
    @GetMapping("/search")
    public ResponseEntity<List<Map<String, Object>>> searchNodes(@RequestParam String term) {
        try {
            return ResponseEntity.ok(graphService.searchNodes(term));
        } catch (Exception e) {
            System.err.println("Error during node search: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.status(500).body(Collections.emptyList());
        }
    }

    /**
     * NEW: Gets initial nodes and relationships by label.
     * @param label The node label.
     * @param limit Optional limit for initial nodes (defaults to 5).
     * @return Map containing nodes and edges for Vis.js.
     */
    @GetMapping("/nodes-by-label")
    public ResponseEntity<Map<String, List<Map<String, Object>>>> getNodesByLabel(
            @RequestParam String label,
            @RequestParam(required = false) Integer limit) {
        try {
            int effectiveLimit = (limit != null && limit > 0) ? limit : DEFAULT_LABEL_LOAD_LIMIT;
            return ResponseEntity.ok(graphService.getNodesByLabel(label, effectiveLimit));
        } catch (Exception e) {
            System.err.println("Error getting nodes by label: " + e.getMessage());
            e.printStackTrace();
            Map<String, List<Map<String, Object>>> errorResult = new HashMap<>();
            errorResult.put("nodes", Collections.emptyList());
            errorResult.put("edges", Collections.emptyList());
            errorResult.put("error", Collections.singletonList(Collections.singletonMap("message", e.getMessage())));
            return ResponseEntity.status(500).body(errorResult);
        }
    }


    /**
     * Expands a node to get its neighbors and relationships.
     * @param nodeId The internal Neo4j ID of the node.
     * @return Map containing nodes and edges for Vis.js.
     */
    @GetMapping("/expand")
    public ResponseEntity<Map<String, List<Map<String, Object>>>> expandNode(@RequestParam long nodeId) {
        try {
            return ResponseEntity.ok(graphService.expandNode(nodeId));
        } catch (Exception e) {
            System.err.println("Error during node expansion: " + e.getMessage());
            e.printStackTrace();
            Map<String, List<Map<String, Object>>> errorResult = Collections.singletonMap("error", Collections.singletonList(Collections.singletonMap("message", e.getMessage())));
            return ResponseEntity.status(500).body(errorResult);
        }
    }

    /**
     * Endpoint to get schema information (labels and relationship types).
     * @return Map containing lists of labels and relationship types with counts.
     */
    @GetMapping("/schema")
    public ResponseEntity<Map<String, List<Map<String, Object>>>> getSchema() {
        try {
            return ResponseEntity.ok(graphService.getSchemaInfo());
        } catch (Exception e) {
            System.err.println("Error fetching graph schema: " + e.getMessage());
            e.printStackTrace();
            Map<String, List<Map<String, Object>>> errorResult = new HashMap<>();
            errorResult.put("labels", Collections.emptyList());
            errorResult.put("relationshipTypes", Collections.emptyList());
            errorResult.put("error", Collections.singletonList(Collections.singletonMap("message", e.getMessage())));
            return ResponseEntity.status(500).body(errorResult);
        }
    }
}
