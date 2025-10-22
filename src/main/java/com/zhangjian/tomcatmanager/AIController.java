package com.zhangjian.tomcatmanager;

import com.zhangjian.tomcatmanager.dto.AIPrompt;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/ai")
public class AIController {

    private final AIService aiService;

    public AIController(AIService aiService) {
        this.aiService = aiService;
    }

    @GetMapping("/prompts")
    public List<AIPrompt> getPrompts() {
        return aiService.getAllPrompts();
    }

    @PostMapping("/prompts")
    public AIPrompt savePrompt(@RequestBody AIPrompt prompt) throws IOException {
        return aiService.savePrompt(prompt);
    }

    @PostMapping("/query")
    public Map<String, Object> performQuery(@RequestBody Map<String, String> payload) {
        String query = payload.get("query");
        String context = payload.get("context");
        return aiService.getMockedAIResponse(query, context);
    }
}

