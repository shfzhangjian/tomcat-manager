package com.zhangjian.tomcatmanager.graph;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Map;

@RestController
@RequestMapping("/api/graph")
public class GraphController {

    private final GraphService graphService;

    public GraphController(GraphService graphService) {
        this.graphService = graphService;
    }

    @GetMapping("/search")
    public Map<String, Object> search(@RequestParam String term) {
        // Placeholder implementation
        return Collections.singletonMap("message", "Search functionality not yet implemented.");
    }

    @GetMapping("/expand")
    public Map<String, Object> expand(@RequestParam String nodeId) {
        // Placeholder implementation
        return Collections.singletonMap("message", "Expand functionality not yet implemented.");
    }
}
