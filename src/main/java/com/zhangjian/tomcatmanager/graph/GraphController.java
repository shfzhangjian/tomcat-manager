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

    // Constructor injection
    public GraphController(GraphService graphService) {
        this.graphService = graphService;
    }

    /**
     * Searches nodes based on a term.
     * @param term Search term.
     * @return List of matching nodes.
     */
    @GetMapping("/search")
    public ResponseEntity<List<Map<String, Object>>> searchNodes(@RequestParam String term) {
        try {
            return ResponseEntity.ok(graphService.searchNodes(term));
        } catch (Exception e) {
            // Log the error server-side
            System.err.println("Error during node search: " + e.getMessage());
            e.printStackTrace();
            // Return an empty list or an error structure to the client
            return ResponseEntity.status(500).body(Collections.emptyList());
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
            // Log the error server-side
            System.err.println("Error during node expansion: " + e.getMessage());
            e.printStackTrace();
            // Return an empty structure or an error structure to the client
            Map<String, List<Map<String, Object>>> errorResult = Collections.singletonMap("error", Collections.singletonList(Collections.singletonMap("message", e.getMessage())));
            return ResponseEntity.status(500).body(errorResult);
        }
    }

    /**
     * NEW: Endpoint to get schema information (labels and relationship types).
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
