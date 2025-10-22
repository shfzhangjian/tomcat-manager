package com.zhangjian.tomcatmanager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhangjian.tomcatmanager.dto.AIPrompt;
import com.zhangjian.tomcatmanager.dto.QueryColumnInfo;
import com.zhangjian.tomcatmanager.dto.QueryResult;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AIService {

    private final ObjectMapper objectMapper;
    private final File promptsFile = new File("ai_prompts.json");
    private final Map<String, AIPrompt> prompts = new LinkedHashMap<>();

    public AIService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        loadPrompts();
    }

    public List<AIPrompt> getAllPrompts() {
        return new ArrayList<>(prompts.values());
    }

    public AIPrompt savePrompt(AIPrompt prompt) throws IOException {
        if (prompt.getId() == null || prompt.getId().isEmpty()) {
            prompt.setId(UUID.randomUUID().toString());
        }
        prompts.put(prompt.getId(), prompt);
        persistPrompts();
        return prompt;
    }

    public Map<String, Object> getMockedAIResponse(String query, String context) {
        // Simulate AI processing delay
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        String sql;
        String naturalLanguageResponse;
        QueryResult queryResult;

        if (query.contains("用户")) {
            sql = "SELECT USER_ID, USER_NAME, DEPARTMENT FROM USERS;";
            naturalLanguageResponse = "好的，这是查询所有用户的 SQL 语句和模拟结果。";
            List<String> headers = Arrays.asList("USER_ID", "USER_NAME", "DEPARTMENT");
            List<Map<String, Object>> rows = Arrays.asList(
                    createRow(headers, "101", "张三", "研发部"),
                    createRow(headers, "102", "李四", "产品部")
            );
            // FIX: Convert List<String> to List<QueryColumnInfo> and call new success method
            List<QueryColumnInfo> columnInfo = headers.stream()
                    .map(h -> new QueryColumnInfo(h, null))
                    .collect(Collectors.toList());
            queryResult = QueryResult.success(columnInfo, rows, 0, 1, rows.size());

        } else if (query.contains("部门") && query.contains("人数")) {
            sql = "SELECT DEPARTMENT, COUNT(*) as EMPLOYEE_COUNT FROM USERS GROUP BY DEPARTMENT;";
            naturalLanguageResponse = "当然，这是按部门统计员工人数的 SQL 和模拟结果。";
            List<String> headers = Arrays.asList("DEPARTMENT", "EMPLOYEE_COUNT");
            List<Map<String, Object>> rows = Arrays.asList(
                    createRow(headers, "研发部", 25),
                    createRow(headers, "产品部", 15),
                    createRow(headers, "市场部", 10)
            );
            // FIX: Convert List<String> to List<QueryColumnInfo> and call new success method
            List<QueryColumnInfo> columnInfo = headers.stream()
                    .map(h -> new QueryColumnInfo(h, null))
                    .collect(Collectors.toList());
            queryResult = QueryResult.success(columnInfo, rows, 0, 1, rows.size());
        } else {
            sql = "-- 无法为此问题生成 SQL。";
            naturalLanguageResponse = "抱歉，我暂时无法理解您的问题。您可以尝试更具体一些，或者选择一个相关的“问数主题”。";
            queryResult = QueryResult.error("AI无法生成有效的SQL。");
        }

        Map<String, Object> response = new HashMap<>();
        response.put("sql", sql);
        response.put("naturalLanguageResponse", naturalLanguageResponse);
        response.put("queryResult", queryResult);
        response.put("contextReceived", context != null && !context.isEmpty());

        return response;
    }

    private Map<String, Object> createRow(List<String> headers, Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            row.put(headers.get(i), values[i]);
        }
        return row;
    }

    private void loadPrompts() {
        if (promptsFile.exists()) {
            try {
                List<AIPrompt> loaded = objectMapper.readValue(promptsFile, new TypeReference<List<AIPrompt>>() {});
                prompts.clear();
                loaded.forEach(p -> prompts.put(p.getId(), p));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void persistPrompts() throws IOException {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(promptsFile, new ArrayList<>(prompts.values()));
    }
}
