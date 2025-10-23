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
// import java.util.stream.Collectors; // No longer needed for stream() on Iterable

@Service
public class GraphService {

    private static final Logger logger = LoggerFactory.getLogger(GraphService.class);
    private final Driver neo4jDriver;

    // Constructor injection
    public GraphService(Driver neo4jDriver) {
        this.neo4jDriver = neo4jDriver;
    }

    /**
     * Searches for nodes in Neo4j based on a term, matching against common properties.
     * @param term The search term.
     * @return A list of maps, each representing a node with id, labels, and properties.
     */
    public List<Map<String, Object>> searchNodes(String term) {
        // --- START DEBUG LOGGING ---
        logger.info(">>> GraphService.searchNodes received term: '{}'", term);
        // --- END DEBUG LOGGING ---

        // Use try-with-resources for automatic session closing
        try (Session session = neo4jDriver.session()) {
            // Updated Cypher query for broader search and case-insensitivity
            String cypher = "MATCH (n) " +
                    "WHERE toLower(toString(n.name)) CONTAINS toLower($term) " +
                    "   OR toLower(toString(n.id)) CONTAINS toLower($term) " +
                    "   OR toLower(toString(n.locationCode)) CONTAINS toLower($term) " + // Added locationCode
                    // ADDED: Also search if the term matches a label directly
                    "   OR $term IN labels(n) " +
                    "RETURN id(n) AS id, labels(n) AS labels, properties(n) AS properties " +
                    "LIMIT 25"; // Limit results for performance

            // --- START DEBUG LOGGING ---
            logger.info(">>> Executing Cypher query: {}", cypher);
            logger.info(">>> With parameters: term={}", term);
            // --- END DEBUG LOGGING ---

            Result result = session.run(cypher, Values.parameters("term", term));
            List<Map<String, Object>> nodes = new ArrayList<>();
            int count = 0; // Count results for logging
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

                // --- START DEBUG LOGGING (Log first few results) ---
                if (count <= 3) {
                    logger.debug(">>> Found node: id={}, labels={}, properties={}", nodeId, labels, properties);
                } else if (count == 4) {
                    logger.debug(">>> (More results follow...)");
                }
                // --- END DEBUG LOGGING ---
            }

            // --- START DEBUG LOGGING ---
            logger.info(">>> Query finished. Found {} nodes.", count);
            logger.info(">>> Returning {} nodes to controller.", nodes.size());
            // --- END DEBUG LOGGING ---

            return nodes;
        } catch (Exception e) {
            // --- START DEBUG LOGGING ---
            logger.error("!!! Error searching nodes in Neo4j: {}", e.getMessage(), e);
            // --- END DEBUG LOGGING ---
            // In case of error, return an empty list or throw a custom exception
            return Collections.emptyList();
        }
    }

    /**
     * Expands a node in Neo4j, returning its direct neighbors and relationships in Vis.js format.
     * @param nodeId The internal Neo4j ID of the node to expand.
     * @return A map containing lists of nodes and edges for Vis.js.
     */
    public Map<String, List<Map<String, Object>>> expandNode(long nodeId) {
        logger.info(">>> GraphService.expandNode received nodeId: {}", nodeId);
        try (Session session = neo4jDriver.session()) {
            // Cypher query to get the node and its direct neighbors and relationships
            String cypher = "MATCH (n)-[r]-(m) WHERE id(n) = $nodeId RETURN n, r, m";
            logger.info(">>> Executing Cypher query: {}", cypher);
            logger.info(">>> With parameters: nodeId={}", nodeId);
            Result result = session.run(cypher, Values.parameters("nodeId", nodeId));
            Map<String, List<Map<String, Object>>> graphData = convertResultToVisFormat(result);
            logger.info(">>> Expansion query finished. Returning {} nodes and {} edges.",
                    graphData.get("nodes").size(), graphData.get("edges").size());
            return graphData;
        } catch (Exception e) {
            logger.error("!!! Error expanding node {} in Neo4j: {}", nodeId, e.getMessage(), e);
            // Return an empty structure in case of error
            Map<String, List<Map<String, Object>>> emptyResult = new HashMap<>();
            emptyResult.put("nodes", Collections.emptyList());
            emptyResult.put("edges", Collections.emptyList());
            return emptyResult;
        }
    }

    /**
     * Gets schema information (labels, relationship types, and counts) from Neo4j.
     * @return A map containing lists of labels and relationship types with their counts.
     */
    public Map<String, List<Map<String, Object>>> getSchemaInfo() {
        logger.info(">>> GraphService.getSchemaInfo called.");
        Map<String, List<Map<String, Object>>> schema = new HashMap<>();
        schema.put("labels", new ArrayList<>());
        schema.put("relationshipTypes", new ArrayList<>());

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


    /**
     * Converts Neo4j query results into Vis.js compatible format.
     * @param result The Neo4j Result object.
     * @return A map containing lists of nodes and edges.
     */
    private Map<String, List<Map<String, Object>>> convertResultToVisFormat(Result result) {
        Map<String, List<Map<String, Object>>> graphData = new HashMap<>();
        Set<Long> nodeIds = new HashSet<>();
        Set<Long> edgeIds = new HashSet<>();
        List<Map<String, Object>> nodes = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();
        int recordCount = 0;

        while (result.hasNext()) {
            recordCount++;
            Record record = result.next();

            // Process node 'n'
            Value nValue = record.get("n");
            if (nValue instanceof NodeValue) {
                Node n = nValue.asNode();
                if (nodeIds.add(n.id())) { // Add only if not already added
                    nodes.add(convertNodeToMap(n));
                }
            }

            // Process node 'm'
            Value mValue = record.get("m");
            if (mValue instanceof NodeValue) {
                Node m = mValue.asNode();
                if (nodeIds.add(m.id())) { // Add only if not already added
                    nodes.add(convertNodeToMap(m));
                }
            }

            // Process relationship 'r'
            Value rValue = record.get("r");
            if (rValue instanceof RelationshipValue) {
                Relationship r = rValue.asRelationship();
                if (edgeIds.add(r.id())) { // Add only if not already added
                    edges.add(convertRelationshipToMap(r));
                }
            }
        }
        logger.debug(">>> Processed {} records from expansion query.", recordCount);
        graphData.put("nodes", nodes);
        graphData.put("edges", edges);
        return graphData;
    }

    /**
     * Helper to convert a Neo4j Node to a Map.
     */
    private Map<String, Object> convertNodeToMap(Node node) {
        Map<String, Object> nodeMap = new HashMap<>();
        nodeMap.put("id", node.id());
        // FIX: Convert Iterable<String> to List<String> for Java 8 compatibility
        List<String> labels = new ArrayList<>();
        for (String label : node.labels()) {
            labels.add(label);
        }
        nodeMap.put("labels", labels);
        nodeMap.put("properties", node.asMap());
        return nodeMap;
    }

    /**
     * Helper to convert a Neo4j Relationship to a Map.
     */
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

