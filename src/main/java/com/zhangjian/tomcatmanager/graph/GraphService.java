/**
 * 文件路径: src/main/java/com/zhangjian/tomcatmanager/graph/GraphService.java
 * 修改说明 v2.8 (JDK 1.8):
 * 1. 修正 searchNodes 中属性搜索的 Cypher 构建逻辑，不再参数化属性名，改为动态构建并使用反引号包裹。
 * 2. 在 getDefaultSearchCypher 中添加对中文属性名 "制造商" 的搜索支持。
 * 3. 确保参数传递正确。
 */
package com.zhangjian.tomcatmanager.graph;

import org.neo4j.driver.*;
import org.neo4j.driver.Record; // Explicit import
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class GraphService {

    private static final Logger logger = LoggerFactory.getLogger(GraphService.class);
    private final Driver neo4jDriver;
    private static final int DEFAULT_LABEL_LOAD_LIMIT = 5; // 默认加载数量

    // Constructor injection
    public GraphService(Driver neo4jDriver) {
        this.neo4jDriver = neo4jDriver;
    }

    /**
     * Searches for nodes in Neo4j based on a term.
     * Supports general search or property-specific search (e.g., "propertyName:value").
     * @param term The search term.
     * @return A list of maps, each representing a node with id, labels, and properties.
     */
    public List<Map<String, Object>> searchNodes(String term) {
        logger.info(">>> GraphService.searchNodes received term: '{}'", term);

        String cypher;
        Map<String, Object> params = new HashMap<>();

        // 检查是否为属性搜索
        if (term != null && term.contains(":")) {
            String[] parts = term.split(":", 2);
            // 确保属性名和值都不为空
            if (parts.length == 2 && !parts[0].trim().isEmpty() && !parts[1].trim().isEmpty()) {
                String propName = parts[0].trim();
                String propValue = parts[1].trim();
                // **修正:** 动态构建 Cypher 字符串，并用反引号包裹属性名
                // 注意: 清理属性名以防止注入，例如只允许字母数字下划线或中文
                String sanitizedPropName = propName.replaceAll("[^\\w\\u4e00-\\u9fa5]", ""); // 允许字母数字下划线和中文
                if (!sanitizedPropName.isEmpty()) {
                    cypher = String.format(
                            "MATCH (n) WHERE toLower(toString(n.`%s`)) CONTAINS toLower($propValue) " +
                                    "RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties " +
                                    "LIMIT 25", sanitizedPropName); // 使用清理后的属性名
                    params.put("propValue", propValue);
                    logger.info(">>> Performing property search for property '{}'", sanitizedPropName);
                } else {
                    // 无效属性名，回退到通用搜索
                    cypher = getDefaultSearchCypher();
                    params.put("term", term); // 仍然使用原始 term 进行通用搜索
                    logger.info(">>> Invalid property name for search, falling back to general search.");
                }
            } else {
                // 格式无效，回退到通用搜索
                cypher = getDefaultSearchCypher();
                params.put("term", term);
                logger.info(">>> Invalid property search format, falling back to general search.");
            }
        } else {
            // 通用搜索
            cypher = getDefaultSearchCypher();
            params.put("term", term == null ? "" : term); // Ensure term is not null for query
            logger.info(">>> Performing general search.");
        }


        try (Session session = neo4jDriver.session()) {
            logger.info(">>> Executing Cypher query: {}", cypher);
            logger.info(">>> With parameters: {}", params);

            Result result = session.run(cypher, params);
            List<Map<String, Object>> nodes = new ArrayList<>();
            int count = 0;
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> nodeMap = new HashMap<>();
                long nodeId = record.get("id").asLong();
                List<String> labels = record.get("labels").asList(Value::asString);
                Map<String, Object> properties = record.get("properties").asMap();

                nodeMap.put("id", nodeId);
                nodeMap.put("labels", labels);
                nodeMap.put("properties", properties);
                nodes.add(nodeMap);
                count++;
                if (count <= 3) {
                    logger.debug(">>> Found node: id={}, labels={}, properties={}", nodeId, labels, properties);
                } else if (count == 4) {
                    logger.debug(">>> (More results follow...)");
                }
            }

            logger.info(">>> Query finished. Found {} nodes.", count);
            logger.info(">>> Returning {} nodes to controller.", nodes.size());

            return nodes;
        } catch (Exception e) {
            logger.error("!!! Error searching nodes in Neo4j: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    // Helper method for the default search Cypher query
    private String getDefaultSearchCypher() {
        return "MATCH (n) " +
                "WHERE toLower(toString(n.name)) CONTAINS toLower($term) " +
                "   OR toLower(toString(n.id)) CONTAINS toLower($term) " +
                "   OR toLower(toString(n.`名称`)) CONTAINS toLower($term) " +
                "   OR toLower(toString(n.`位置编号`)) CONTAINS toLower($term) " +
                "   OR toLower(toString(n.`制造商`)) CONTAINS toLower($term) " + // **新增: 搜索制造商**
                "   OR toLower(toString(n.locationCode)) CONTAINS toLower($term) " +
                "   OR $term IN labels(n) " +
                "RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties " +
                "LIMIT 25";
    }

    // ... (getNodesByLabel, expandNode, convertResult*, getSchemaInfo, createEmptyGraphData, convertNodeToMap, convertRelationshipToMap 方法保持不变) ...

    /**
     * NEW: Gets the initial set of nodes for a given label, including their first-level relationships,
     * and indicates if connected nodes have further relationships.
     * @param label The node label to query.
     * @param limit The maximum number of initial nodes to return.
     * @return A map containing lists of nodes and edges for Vis.js.
     */
    public Map<String, List<Map<String, Object>>> getNodesByLabel(String label, int limit) {
        logger.info(">>> GraphService.getNodesByLabel called for label: {}, limit: {}", label, limit);
        if (label == null || label.trim().isEmpty()) {
            logger.warn("Label cannot be empty for getNodesByLabel.");
            return createEmptyGraphData();
        }
        if (limit <= 0) {
            limit = DEFAULT_LABEL_LOAD_LIMIT;
        }

        try (Session session = neo4jDriver.session()) {
            // 使用参数化查询防止标签注入
            String cypher =
                    "MATCH (n) WHERE $label IN labels(n) " + // Match initial nodes by label using parameter
                            "WITH n LIMIT $limit " + // Limit the initial nodes
                            "OPTIONAL MATCH (n)-[r]-(m) " + // Match their direct relationships and neighbors
                            "WITH n, r, m, id(n) as nid, id(m) as mid " + // Include IDs for distinct collection
                            "OPTIONAL MATCH (m)-[r2]-() WHERE id(m) <> nid " + // Check if neighbors have *other* relationships
                            "RETURN DISTINCT n, r, m, nid, mid, count(DISTINCT r2) > 0 AS mHasMoreRels";

            Map<String, Object> params = new HashMap<>();
            params.put("label", label);
            params.put("limit", limit);

            logger.info(">>> Executing Cypher query for getNodesByLabel: {}", cypher);
            logger.info(">>> With parameters: {}", params);
            Result result = session.run(cypher, params);

            Map<String, List<Map<String, Object>>> graphData = convertResultWithExpandFlagToVisFormat(result);
            logger.info(">>> Initial label load query finished. Returning {} nodes and {} edges.",
                    graphData.get("nodes").size(), graphData.get("edges").size());
            return graphData;

        } catch (Exception e) {
            logger.error("!!! Error getting nodes by label '{}' in Neo4j: {}", label, e.getMessage(), e);
            return createEmptyGraphData();
        }
    }


    /**
     * Expands a node in Neo4j, returning its direct neighbors and relationships in Vis.js format,
     * including a flag indicating if neighbors have further relationships.
     * @param nodeId The internal Neo4j ID of the node to expand.
     * @return A map containing lists of nodes and edges for Vis.js.
     */
    public Map<String, List<Map<String, Object>>> expandNode(long nodeId) {
        logger.info(">>> GraphService.expandNode received nodeId: {}", nodeId);
        try (Session session = neo4jDriver.session()) {
            // Cypher query to get the node and its direct neighbors and relationships
            // Also check if the neighbors (m) have more relationships
            String cypher = "MATCH (n)-[r]-(m) WHERE id(n) = $nodeId " +
                    "WITH n, r, m, id(n) as nid, id(m) as mid " +
                    "OPTIONAL MATCH (m)-[r2]-() WHERE id(m) <> nid " + // Check for *other* relationships of m
                    "RETURN DISTINCT n, r, m, nid, mid, count(DISTINCT r2) > 0 AS mHasMoreRels";

            logger.info(">>> Executing Cypher query: {}", cypher);
            logger.info(">>> With parameters: nodeId={}", nodeId);
            Result result = session.run(cypher, Values.parameters("nodeId", nodeId));

            Map<String, List<Map<String, Object>>> graphData = convertResultWithExpandFlagToVisFormat(result);
            logger.info(">>> Expansion query finished. Returning {} nodes and {} edges.",
                    graphData.get("nodes").size(), graphData.get("edges").size());
            return graphData;
        } catch (Exception e) {
            logger.error("!!! Error expanding node {} in Neo4j: {}", nodeId, e.getMessage(), e);
            // 修正: 确保返回正确的类型 Map<String, List<Map<String, Object>>>
            return createEmptyGraphData();
        }
    }

    /**
     * Converts Neo4j query results (including the expand flag) into Vis.js compatible format.
     * @param result The Neo4j Result object.
     * @return A map containing lists of nodes and edges.
     */
    private Map<String, List<Map<String, Object>>> convertResultWithExpandFlagToVisFormat(Result result) {
        Map<String, List<Map<String, Object>>> graphData = new HashMap<>();
        Set<Long> nodeIdsProcessed = new HashSet<>(); // 跟踪已处理并添加到 nodes 列表的节点ID
        Set<Long> edgeIdsProcessed = new HashSet<>();
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();
        Map<Long, Boolean> nodeExpandableFlags = new HashMap<>(); // 存储邻居节点的展开标志
        Map<Long, Map<String, Object>> nodeDataCache = new HashMap<>(); // 缓存节点数据以供后续查找
        int recordCount = 0;

        while (result.hasNext()) {
            recordCount++;
            Record record = result.next();

            Value nValue = record.get("n");
            Value rValue = record.get("r");
            Value mValue = record.get("m");
            boolean mHasMoreRels = record.get("mHasMoreRels").asBoolean(false);

            Node n = null;
            Node m = null;

            // 处理节点 'n'
            if (nValue != null && !nValue.isNull() && nValue instanceof NodeValue) {
                n = nValue.asNode();
                long nid = n.id();
                if (!nodeDataCache.containsKey(nid)) {
                    nodeDataCache.put(nid, convertNodeToMap(n)); // 缓存 'n' 的数据
                }
                // 'n' 的 hasMoreRelationships 标志在后面统一处理
            }

            // 处理节点 'm' (邻居)
            if (mValue != null && !mValue.isNull() && mValue instanceof NodeValue) {
                m = mValue.asNode();
                long mid = m.id();
                if (!nodeDataCache.containsKey(mid)) {
                    nodeDataCache.put(mid, convertNodeToMap(m)); // 缓存 'm' 的数据
                }
                // 存储或更新 'm' 的可扩展标志
                // 如果 m 被多次引用，只要有一次 mHasMoreRels 为 true，就标记为 true
                nodeExpandableFlags.put(mid, nodeExpandableFlags.containsKey(mid) ? (nodeExpandableFlags.get(mid) || mHasMoreRels) : mHasMoreRels);
            }

            // 处理关系 'r'
            if (rValue != null && !rValue.isNull() && rValue instanceof RelationshipValue) {
                Relationship r = rValue.asRelationship();
                if (edgeIdsProcessed.add(r.id())) {
                    edges.add(convertRelationshipToMap(r));
                }
            }
        }

        // 统一处理所有缓存的节点数据，添加 hasMoreRelationships 标志并加入最终列表
        for (Map.Entry<Long, Map<String, Object>> entry : nodeDataCache.entrySet()) {
            long nodeId = entry.getKey();
            Map<String, Object> nodeMap = entry.getValue();
            // 从 nodeExpandableFlags 获取标志，如果不存在则默认为 false
            boolean hasMore = nodeExpandableFlags.containsKey(nodeId) ? nodeExpandableFlags.get(nodeId) : false;
            nodeMap.put("hasMoreRelationships", hasMore);
            if (nodeIdsProcessed.add(nodeId)) { // 避免重复添加
                nodes.add(nodeMap);
            }
        }


        logger.debug(">>> Processed {} records from expansion query.", recordCount);
        graphData.put("nodes", nodes);
        graphData.put("edges", edges);
        return graphData;
    }


    /**
     * Helper to convert a Neo4j Node to a Map, including the expand flag.
     * Note: This overloaded method might not be strictly needed anymore after refactoring convertResultWithExpandFlagToVisFormat
     * but kept for potential direct use if needed.
     */
    private Map<String, Object> convertNodeToMap(Node node, boolean hasMoreRelationships) {
        Map<String, Object> nodeMap = convertNodeToMap(node); // Reuse existing conversion
        nodeMap.put("hasMoreRelationships", hasMoreRelationships);
        return nodeMap;
    }


    // --- Existing Methods ---

    public Map<String, List<Map<String, Object>>> getSchemaInfo() {
        // ... (existing getSchemaInfo implementation remains the same)
        logger.info(">>> GraphService.getSchemaInfo called.");
        Map<String, List<Map<String, Object>>> schema = new HashMap<>();
        schema.put("labels", new ArrayList<Map<String, Object>>()); // Use explicit type for JDK 1.8
        schema.put("relationshipTypes", new ArrayList<Map<String, Object>>()); // Use explicit type for JDK 1.8

        try (Session session = neo4jDriver.session()) {
            // Get Node Labels and Counts (Requires APOC for efficient counts)
            String labelsQuery = "CALL db.labels() YIELD label " +
                    "CALL apoc.cypher.run('MATCH (:`' + label + '`) RETURN count(*) AS count', {}) YIELD value " +
                    "RETURN label, value.count AS count ORDER BY label";
            logger.info(">>> Executing Schema (Labels) query: {}", labelsQuery);
            Result labelsResult = session.run(labelsQuery);
            while (labelsResult.hasNext()) {
                Record record = labelsResult.next();
                Map<String, Object> labelInfo = new HashMap<>();
                labelInfo.put("name", record.get("label").asString());
                labelInfo.put("count", record.get("count").asLong());
                schema.get("labels").add(labelInfo);
            }
            logger.info(">>> Found {} node labels with counts.", schema.get("labels").size());

            // Get Relationship Types and Counts (Requires APOC for efficient counts)
            String relTypesQuery = "CALL db.relationshipTypes() YIELD relationshipType " +
                    "CALL apoc.cypher.run('MATCH ()-[:`' + relationshipType + '`]->() RETURN count(*) AS count', {}) YIELD value " +
                    "RETURN relationshipType, value.count AS count ORDER BY relationshipType";
            logger.info(">>> Executing Schema (Relationships) query: {}", relTypesQuery);
            Result relTypesResult = session.run(relTypesQuery);
            while (relTypesResult.hasNext()) {
                Record record = relTypesResult.next();
                Map<String, Object> relTypeInfo = new HashMap<>();
                relTypeInfo.put("name", record.get("relationshipType").asString());
                relTypeInfo.put("count", record.get("count").asLong());
                schema.get("relationshipTypes").add(relTypeInfo);
            }
            logger.info(">>> Found {} relationship types with counts.", schema.get("relationshipTypes").size());

        } catch (Exception e) {
            // Log the error but return potentially partial results or just names
            logger.warn("!!! Error fetching schema info with counts from Neo4j: {}. APOC functions might be needed for counts. Falling back to names only.", e.getMessage());
            // Fallback: Attempt to get just names if counts fail
            try (Session session = neo4jDriver.session()) {
                // Only fetch names if the previous attempt failed to populate the list
                if (schema.get("labels").isEmpty()) {
                    logger.info(">>> Falling back to fetch label names only.");
                    Result labelsResult = session.run("CALL db.labels() YIELD label RETURN label ORDER BY label");
                    while (labelsResult.hasNext()) {
                        Map<String, Object> labelInfo = new HashMap<>();
                        labelInfo.put("name", labelsResult.next().get("label").asString());
                        labelInfo.put("count", -1L); // Indicate count unavailable
                        schema.get("labels").add(labelInfo);
                    }
                    logger.info(">>> Found {} node label names.", schema.get("labels").size());
                }
                if (schema.get("relationshipTypes").isEmpty()) {
                    logger.info(">>> Falling back to fetch relationship type names only.");
                    Result relTypesResult = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType");
                    while (relTypesResult.hasNext()) {
                        Map<String, Object> relTypeInfo = new HashMap<>();
                        relTypeInfo.put("name", relTypesResult.next().get("relationshipType").asString());
                        relTypeInfo.put("count", -1L); // Indicate count unavailable
                        schema.get("relationshipTypes").add(relTypeInfo);
                    }
                    logger.info(">>> Found {} relationship type names.", schema.get("relationshipTypes").size());
                }
            } catch (Exception innerE) {
                logger.error("!!! Error fetching basic schema names from Neo4j: {}", innerE.getMessage(), innerE);
            }
        }
        logger.info(">>> Returning schema info.");
        return schema;
    }

    // Existing helper methods
    private Map<String, List<Map<String, Object>>> createEmptyGraphData() {
        Map<String, List<Map<String, Object>>> emptyResult = new HashMap<>();
        emptyResult.put("nodes", Collections.<Map<String, Object>>emptyList()); // Explicit type for JDK 1.8
        emptyResult.put("edges", Collections.<Map<String, Object>>emptyList()); // Explicit type for JDK 1.8
        return emptyResult;
    }

    /**
     * Helper to convert a Neo4j Node to a Map (base conversion without expand flag).
     */
    private Map<String, Object> convertNodeToMap(Node node) {
        Map<String, Object> nodeMap = new HashMap<>();
        nodeMap.put("id", node.id());
        List<String> labels = new ArrayList<>();
        // Use traditional for loop for JDK 1.8 compatibility
        for (String label : node.labels()) {
            labels.add(label);
        }
        nodeMap.put("labels", labels);
        nodeMap.put("properties", node.asMap());
        return nodeMap;
    }

    private Map<String, Object> convertRelationshipToMap(Relationship relationship) {
        Map<String, Object> edgeMap = new HashMap<>();
        edgeMap.put("id", relationship.id());
        edgeMap.put("startNodeId", relationship.startNodeId());
        edgeMap.put("endNodeId", relationship.endNodeId());
        edgeMap.put("type", relationship.type());
        edgeMap.put("properties", relationship.asMap());
        return edgeMap;
    }
}

