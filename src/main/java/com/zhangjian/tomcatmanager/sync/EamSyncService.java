package com.zhangjian.tomcatmanager.sync;

import com.zhangjian.tomcatmanager.DatabaseService;
import com.zhangjian.tomcatmanager.sync.model.MappingConfig;
import com.zhangjian.tomcatmanager.sync.model.NodeMapping;
import com.zhangjian.tomcatmanager.sync.model.RelationshipMapping;
import com.zhangjian.tomcatmanager.sync.model.SyncStatus;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Main service for handling the EAM to Neo4j synchronization logic.
 */
@Service
public class EamSyncService {

    private final DatabaseService databaseService;
    private final SyncLogService syncLogService;
    private final MappingConfigLoader mappingConfigLoader;
    private final Driver neo4jDriver;
    private final Map<String, SyncStatus> syncStatusMap = new ConcurrentHashMap<>();

    private static final int BATCH_SIZE = 1000;

    private static final String DEFAULT_MAPPING_CONFIG = "" +
            "# 卷接机组 EAM 数据到 Neo4j 的映射配置\n" +
            "# version: 1.0\n" +
            "\n" +
            "# 1. 定义节点 (Nodes)\n" +
            "# 定义需要从 Oracle 表抽取并创建为 Neo4j 节点的实体。\n" +
            "nodes:\n" +
            "  # 定义“卷接机组”节点\n" +
            "  unit:\n" +
            "    label: Unit                # 在 Neo4j 中的标签\n" +
            "    source: EQJZ                # 源数据表: 卷接机组表\n" +
            "    primaryKey: INDOCNO        # 主键，用于MERGE操作，防止重复创建\n" +
            "    properties:\n" +
            "      id: INDOCNO              # 属性映射：Neo4j属性名: Oracle列名\n" +
            "      name: SJZNAME\n" +
            "\n" +
            "  # 定义“设备”节点\n" +
            "  equipment:\n" +
            "    label: Equipment           # 在 Neo4j 中的标签\n" +
            "    source: EQTREE             # 源数据表: 设备树表\n" +
            "    primaryKey: SFCODE         # 使用设备位置编号作为唯一标识\n" +
            "    properties:\n" +
            "      locationCode: SFCODE\n" +
            "      name: SFNAME\n" +
            "      assetCode: SPMCODE\n" +
            "      installDate: DFIXGMT\n" +
            "      installUnit: SFIXDWN\n" +
            "      category: SECODE\n" +
            "      weight: FWEIGHT\n" +
            "      model: SMTYPE\n" +
            "      spec: SPECPI\n" +
            "      material: SMAKE\n" +
            "      factoryDate: DFACTORY\n" +
            "      factoryNumber: SFACTNO\n" +
            "      manufacturer: SPRODUCT\n" +
            "      supplier: SUPPLIER\n" +
            "\n" +
            "# 2. 定义关系 (Relationships)\n" +
            "# 定义节点之间的关系。\n" +
            "relationships:\n" +
            "  # 关系1: 卷接机组 -> 包含 -> 卷接机 (设备)\n" +
            "  - type: CONSISTS_OF          # 关系类型\n" +
            "    fromNode: unit             # 起始节点 (上面定义的 unit)\n" +
            "    toNode: equipment          # 目标节点 (上面定义的 equipment)\n" +
            "    joinOn: \"SFCODE=SFCODE\"    # 连接条件: unit源表中的SFCODE = equipment源表中的SFCODE\n" +
            "\n" +
            "  # 关系2: 卷接机组 -> 包含 -> 包装机 (设备)\n" +
            "  - type: CONSISTS_OF\n" +
            "    fromNode: unit\n" +
            "    toNode: equipment\n" +
            "    joinOn: \"SFCODE1=SFCODE\"   # 连接条件: unit源表中的SFCODE1 = equipment源表中的SFCODE\n" +
            "\n" +
            "  # 关系3: 设备 -> 包含 -> 子设备 (重要: 需要后端逻辑支持)\n" +
            "  # 注意：基于层次编号的父子关系 (CONTAINS) 是一种复杂的自连接。\n" +
            "  # EamSyncService 已支持 `HIERARCHY:` 标识来处理这种逻辑。\n" +
            "  - type: CONTAINS\n" +
            "    fromNode: equipment\n" +
            "    toNode: equipment\n" +
            "    joinOn: \"HIERARCHY:SFCODE\" # 特殊标识，提示后端使用层次逻辑处理\n";


    public EamSyncService(DatabaseService databaseService, SyncLogService syncLogService, MappingConfigLoader mappingConfigLoader, Driver neo4jDriver) {
        this.databaseService = databaseService;
        this.syncLogService = syncLogService;
        this.mappingConfigLoader = mappingConfigLoader;
        this.neo4jDriver = neo4jDriver;
    }

    public String getMappingConfig(String connectionId) throws IOException {
        Path path = Paths.get("mapping-config-" + connectionId + ".yml");
        if (Files.exists(path)) {
            return new String(Files.readAllBytes(path));
        } else {
            // If no specific config is saved for this connection, return the default template.
            return DEFAULT_MAPPING_CONFIG;
        }
    }

    public void saveMappingConfig(String connectionId, String mappingData) throws IOException {
        Path path = Paths.get("mapping-config-" + connectionId + ".yml");
        // FIX: Use Files.write for Java 8 compatibility
        Files.write(path, mappingData.getBytes());
    }

    @Async
    public void executeSync(String connectionId) {
        long startTime = System.currentTimeMillis();
        updateStatus(connectionId, "IN_PROGRESS", "Synchronization started.", null);
        syncLogService.broadcast(connectionId, "INFO: Synchronization process started for connection " + connectionId);

        try (Connection oracleConn = databaseService.getConnection(connectionId);
             Session session = neo4jDriver.session()) {

            // FIX: Call the correct load method on mappingConfigLoader
            MappingConfig mappingConfig = mappingConfigLoader.load(getMappingConfig(connectionId));

            // 1. Process Nodes
            for (Map.Entry<String, NodeMapping> entry : mappingConfig.getNodes().entrySet()) {
                String nodeKey = entry.getKey();
                NodeMapping nodeMapping = entry.getValue();
                syncLogService.broadcast(connectionId, "INFO: Processing node: " + nodeKey + " (Label: " + nodeMapping.getLabel() + ")");

                String sql = "SELECT * FROM " + nodeMapping.getSource();
                try (Statement stmt = oracleConn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {

                    List<String> cypherQueries = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, Object> rowData = resultSetToMap(rs);
                        cypherQueries.add(buildNodeMergeQuery(nodeMapping, rowData));

                        if (cypherQueries.size() >= BATCH_SIZE) {
                            executeCypherBatch(session, cypherQueries);
                            syncLogService.broadcast(connectionId, "INFO: Processed " + BATCH_SIZE + " nodes for " + nodeKey);
                        }
                    }
                    if (!cypherQueries.isEmpty()) {
                        executeCypherBatch(session, cypherQueries);
                    }
                    syncLogService.broadcast(connectionId, "INFO: Finished processing nodes for " + nodeKey);
                }
            }

            // 2. Process Relationships
            for (RelationshipMapping relMapping : mappingConfig.getRelationships()) {
                syncLogService.broadcast(connectionId, "INFO: Processing relationship: " + relMapping.getType());

                if (relMapping.getJoinOn() != null && relMapping.getJoinOn().startsWith("HIERARCHY:")) {
                    handleHierarchyRelationship(session, relMapping, mappingConfig);
                } else {
                    handleStandardRelationship(oracleConn, session, relMapping, mappingConfig);
                }
            }

            long duration = System.currentTimeMillis() - startTime;
            updateStatus(connectionId, "SUCCESS", "Synchronization completed successfully.", duration);
            syncLogService.broadcast(connectionId, "INFO: Synchronization completed successfully in " + duration + "ms.");

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            String errorMessage = "ERROR: Synchronization failed: " + e.getMessage();
            e.printStackTrace();
            syncLogService.broadcast(connectionId, errorMessage);
            updateStatus(connectionId, "FAIL", e.getMessage(), duration);
        }
    }

    private void handleStandardRelationship(Connection oracleConn, Session session, RelationshipMapping relMapping, MappingConfig mappingConfig) throws Exception {
        NodeMapping fromNodeMapping = mappingConfig.getNodes().get(relMapping.getFromNode());
        NodeMapping toNodeMapping = mappingConfig.getNodes().get(relMapping.getToNode());

        String fromTable = fromNodeMapping.getSource();
        String toTable = toNodeMapping.getSource();

        String[] joinParts = relMapping.getJoinOn().split("=");
        String fromJoinKey = joinParts[0];
        String toJoinKey = joinParts[1];

        // Aliases to avoid column name ambiguity
        String fromAlias = "t1";
        String toAlias = "t2";

        String sql = String.format("SELECT %s.%s AS from_pk, %s.%s AS to_pk FROM %s %s JOIN %s %s ON %s.%s = %s.%s",
                fromAlias, fromNodeMapping.getPrimaryKey(),
                toAlias, toNodeMapping.getPrimaryKey(),
                fromTable, fromAlias,
                toTable, toAlias,
                fromAlias, fromJoinKey,
                toAlias, toJoinKey);

        try (Statement stmt = oracleConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            List<String> cypherQueries = new ArrayList<>();
            while(rs.next()) {
                Map<String, Object> rowData = new HashMap<>();
                rowData.put("from_pk", rs.getObject("from_pk"));
                rowData.put("to_pk", rs.getObject("to_pk"));
                cypherQueries.add(buildRelationshipMergeQuery(relMapping, rowData, mappingConfig));

                if (cypherQueries.size() >= BATCH_SIZE) {
                    executeCypherBatch(session, cypherQueries);
                    syncLogService.broadcast(this.syncStatusMap.keySet().stream().findFirst().orElse(""), "INFO: Processed " + BATCH_SIZE + " relationships for " + relMapping.getType());
                }
            }
            if(!cypherQueries.isEmpty()) {
                executeCypherBatch(session, cypherQueries);
            }
        }
    }

    private void handleHierarchyRelationship(Session session, RelationshipMapping relMapping, MappingConfig mappingConfig) {
        String joinKey = relMapping.getJoinOn().split(":")[1];
        NodeMapping nodeMapping = mappingConfig.getNodes().get(relMapping.getFromNode()); // fromNode and toNode are the same

        // Find the property name for the joinKey from the mapping (e.g., SFCODE -> locationCode)
        String propertyName = nodeMapping.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(joinKey))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(joinKey.toLowerCase());

        String cypher = String.format(
                "MATCH (parent:%s), (child:%s) " +
                        "WHERE child.%s STARTS WITH parent.%s " +
                        "AND size(child.%s) > size(parent.%s) " +
                        "AND NOT EXISTS { " +
                        "  MATCH (intermediate:%s) " +
                        "  WHERE child.%s STARTS WITH intermediate.%s " +
                        "  AND intermediate.%s STARTS WITH parent.%s " +
                        "  AND ID(intermediate) <> ID(parent) " +
                        "  AND ID(intermediate) <> ID(child) " +
                        "} " +
                        "MERGE (parent)-[:%s]->(child)",
                nodeMapping.getLabel(), nodeMapping.getLabel(),
                propertyName, propertyName,
                propertyName, propertyName,
                nodeMapping.getLabel(),
                propertyName, propertyName,
                propertyName, propertyName,
                relMapping.getType()
        );
        syncLogService.broadcast(this.syncStatusMap.keySet().stream().findFirst().orElse(""), "INFO: Building hierarchy for " + joinKey);
        session.run(cypher);
        syncLogService.broadcast(this.syncStatusMap.keySet().stream().findFirst().orElse(""), "INFO: Hierarchy built successfully.");
    }


    private String buildNodeMergeQuery(NodeMapping mapping, Map<String, Object> rowData) {
        String primaryKeyProp = mapping.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(mapping.getPrimaryKey()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(mapping.getPrimaryKey().toLowerCase());

        String propertiesString = mapping.getProperties().entrySet().stream()
                .filter(entry -> rowData.get(entry.getValue()) != null) // Filter out null values
                .map(entry -> entry.getKey() + ": " + formatValueForCypher(rowData.get(entry.getValue())))
                .collect(Collectors.joining(", "));

        if (propertiesString.isEmpty()) {
            // Handle case where all properties might be null, though primary key should exist
            return String.format("MERGE (n:%s {%s: %s})",
                    mapping.getLabel(),
                    primaryKeyProp,
                    formatValueForCypher(rowData.get(mapping.getPrimaryKey())));
        }

        return String.format("MERGE (n:%s {%s: %s}) SET n += {%s}",
                mapping.getLabel(),
                primaryKeyProp,
                formatValueForCypher(rowData.get(mapping.getPrimaryKey())),
                propertiesString);
    }

    private String buildRelationshipMergeQuery(RelationshipMapping relMapping, Map<String, Object> rowData, MappingConfig mappingConfig) {
        NodeMapping fromNode = mappingConfig.getNodes().get(relMapping.getFromNode());
        NodeMapping toNode = mappingConfig.getNodes().get(relMapping.getToNode());

        String fromPkProp = fromNode.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(fromNode.getPrimaryKey()))
                .map(Map.Entry::getKey)
                .findFirst().orElse(fromNode.getPrimaryKey().toLowerCase());

        String toPkProp = toNode.getProperties().entrySet().stream()
                .filter(e -> e.getValue().equalsIgnoreCase(toNode.getPrimaryKey()))
                .map(Map.Entry::getKey)
                .findFirst().orElse(toNode.getPrimaryKey().toLowerCase());


        return String.format("MATCH (from:%s {%s: %s}), (to:%s {%s: %s}) MERGE (from)-[:%s]->(to)",
                fromNode.getLabel(), fromPkProp, formatValueForCypher(rowData.get("from_pk")),
                toNode.getLabel(), toPkProp, formatValueForCypher(rowData.get("to_pk")),
                relMapping.getType());
    }

    private void executeCypherBatch(Session session, List<String> queries) {
        try (Transaction tx = session.beginTransaction()) {
            for (String query : queries) {
                tx.run(query);
            }
            tx.commit();
        }
        queries.clear();
    }


    private Map<String, Object> resultSetToMap(ResultSet rs) throws Exception {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        Map<String, Object> row = new HashMap<>(columns);
        for (int i = 1; i <= columns; ++i) {
            row.put(md.getColumnName(i), rs.getObject(i));
        }
        return row;
    }

    private String formatValueForCypher(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        // Escape single quotes and wrap in single quotes
        return "'" + value.toString().replace("'", "\\'").replace("\"", "\\\"") + "'";
    }

    private void updateStatus(String connectionId, String status, String message, Long duration) {
        SyncStatus currentStatus = syncStatusMap.getOrDefault(connectionId, new SyncStatus());
        currentStatus.setStatus(status);
        // FIX: Use LocalDateTime.now() instead of long
        currentStatus.setLastSyncTime(LocalDateTime.now());
        currentStatus.setLastSyncDurationMs(duration);
        // FIX: Create a new SyncHistory object to pass to addHistory
        currentStatus.addHistory(new SyncStatus.SyncHistory(status, message, duration));
        syncStatusMap.put(connectionId, currentStatus);
    }

    public SyncStatus getSyncStatus(String connectionId) {
        return syncStatusMap.getOrDefault(connectionId, new SyncStatus());
    }
}

