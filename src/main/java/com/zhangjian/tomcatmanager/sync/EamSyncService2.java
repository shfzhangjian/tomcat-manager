package com.zhangjian.tomcatmanager.sync;

import com.zhangjian.tomcatmanager.DatabaseService;
import com.zhangjian.tomcatmanager.sync.model.MappingConfig;
import com.zhangjian.tomcatmanager.sync.model.NodeMapping;
import com.zhangjian.tomcatmanager.sync.model.RelationshipMapping;
import com.zhangjian.tomcatmanager.sync.model.SyncStatus;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Main service for handling the EAM to Neo4j synchronization logic.
 * Implements dynamic labels and a specific multi-step process:
 * 1. Process EQJZ (Unit, initial Maker/Packer nodes, Unit->Device relationships).
 * 2. Recursively process EQTREE children based on SPARNO, updating/creating nodes and relationships.
 */
@Service
public class EamSyncService2 {

    private static final Logger logger = LoggerFactory.getLogger(EamSyncService2.class);
    private final DatabaseService databaseService;
    private final SyncLogService syncLogService;
    private final MappingConfigLoader mappingConfigLoader;
    private final Driver neo4jDriver;
    private final Map<String, SyncStatus> syncStatusMap = new ConcurrentHashMap<>();

    private static final int BATCH_SIZE = 500;

    // Default YAML configuration template (v1.8 - Adjusted for recursive Java flow)
    private static final String DEFAULT_MAPPING_CONFIG = "" +
            "# EAM 数据到 Neo4j 的映射配置 (v1.8 - 适配递归Java流程)\n" +
            "# version: 1.8\n" +
            "nodes:\n" +
            "  # 定义“机组”节点的属性 (由Java代码创建)\n" +
            "  unit:\n" +
            "    label: 机组                # 基础标签 (供参考)\n" +
            "    primaryKey: 机组编号      # Neo4j 主键属性名 (供参考)\n" +
            "    source: EQJZ                # 源表 (供参考)\n" +
            "    properties:\n" +
            "      机组编号: INDOCNO\n" +
            "      名称: SJZNAME\n" +
            "      # 其他 EQJZ 属性可以加在这里，如果想添加到 机组 节点上\n" +
            "\n" +
            "  # 定义“设备”节点的属性和动态标签规则 (由Java代码创建/更新)\n" +
            "  equipment:\n" +
            "    label: 设备                # 基础标签 (由Java代码添加)\n" +
            "    primaryKey: 位置编号      # Neo4j 主键属性名 (由Java代码使用)\n" +
            "    source: EQTREE             # 源表 (用于属性映射)\n" +
            "    dynamicLabelLookup:        # 动态标签规则 (由Java代码使用)\n" +
            "      lookupTable: FUNPTYPE\n" +
            "      keyColumn: SFTYPE\n" +
            "      labelColumn: STYNAME\n" +
            "      lookupKeyColumn: SFTYPE\n" +
            "    properties:                # EQTREE 属性映射 (由Java代码使用)\n" +
            "      位置编号: SFCODE\n" +
            "      名称: SFNAME\n" +
            "      资产编号: SPMCODE\n" +
            "      安装时间: DFIXGMT\n" +
            "      安装单位: SFIXDWN\n" +
            "      拆除时间: DUNFIX\n" +
            "      安装拆除标志: IFSIGN\n" +
            "      分类: SECODE\n" +
            "      重量: FWEIGHT\n" +
            "      计量单位: SMETUN\n" +
            "      尺寸: SMSIZE\n" +
            "      库存号: STORERNO\n" +
            "      采购价格: FPRICE\n" +
            "      货币单位: SCOINDW\n" +
            "      购买时间: DEQBUY\n" +
            "      制造商: SPRODUCT\n" +
            "      型号: SMTYPE\n" +
            "      规格: SPECPI\n" +
            "      标准号: SSTNNO\n" +
            "      图号: SPICNO\n" +
            "      材质: SMAKE\n" +
            "      出厂年月: DFACTORY\n" +
            "      出厂编号: SFACTNO\n" +
            "      供应商: SUPPLIER\n" +
            "      制造商编号: SPRODUCTOR\n" +
            "      维护单位: SERVICE\n" +
            "      设备类型编码: SFTYPE\n" +
            "\n" +
            "relationships:\n" +
            "  # 关系 1 & 2: 机组 -> 包含 -> 设备 (类型名由Java代码使用)\n" +
            "  - type: 包含\n" +
            "\n" +
            "  # 关系 3: 设备 -> 下级 -> 子设备 (类型名由Java代码使用)\n" +
            "  - type: 下级\n";
    // hierarchyRel definition is simplified, only type is needed now
    // fromNode, toNode, joinOn: HIERARCHY are not directly used but kept for clarity


    @Autowired
    public EamSyncService2(DatabaseService databaseService, SyncLogService syncLogService, MappingConfigLoader mappingConfigLoader, Driver neo4jDriver) {
        this.databaseService = databaseService;
        this.syncLogService = syncLogService;
        this.mappingConfigLoader = mappingConfigLoader;
        this.neo4jDriver = neo4jDriver;
    }

    // --- Config Methods ---
    public String getMappingConfig(String connectionId) throws IOException {
        Path path = Paths.get("mapping-config-" + connectionId + ".yml");
        if (Files.exists(path)) {
            logger.info("Loading mapping config from file: {}", path);
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } else {
            logger.info("Mapping config file not found for {}, returning default.", connectionId);
            return DEFAULT_MAPPING_CONFIG;
        }
    }

    public void saveMappingConfig(String connectionId, String mappingData) throws IOException {
        Path path = Paths.get("mapping-config-" + connectionId + ".yml");
        Files.write(path, mappingData.getBytes(StandardCharsets.UTF_8));
        logger.info("Saved mapping config to file: {}", path);
    }

    private Map<String, Long> createHierarchyRelationshipsRecursive(
            Session session, Long parentIdocid, String parentSfcode, // Pass parent IDOCID and SFCODE
            RelationshipMapping hierarchyRel, Map<String, Long> sfcodeToIdocidMap,
            Set<String> processedHierarchySfCodes, // Track processed nodes for relationship creation
            String connectionId, String eqPkProp,
            PreparedStatement findChildrenBySparnoStmt // Reuse prepared statement
    ) {
        long relsProcessedInBranch = 0;
        Map<String, Long> totalCounts = new HashMap<>();
        totalCounts.put("rels", 0L); // Only count relationships here

        if (parentIdocid == null || !processedHierarchySfCodes.add(parentSfcode)) {
            // Stop if parent ID is null or already processed this node for hierarchy
            return totalCounts;
        }

        logDebug(connectionId, "Recursively creating hierarchy relationships for children of: " + parentSfcode + " (IDOCID: " + parentIdocid + ")");

        List<String> childCypherBatch = new ArrayList<>();
        List<String> childSfCodesForNextLevel = new ArrayList<>(); // Store SFCODEs for next recursion

        try {
            findChildrenBySparnoStmt.setLong(1, parentIdocid);
            logDebug(connectionId, "Executing query for child SFCODEs: SELECT SFCODE FROM EQTREE WHERE SPARNO = " + parentIdocid);
            try (ResultSet rsChildren = findChildrenBySparnoStmt.executeQuery()) {
                while (rsChildren.next()) {
                    String childSfcode = rsChildren.getString("SFCODE");
                    if (childSfcode != null) {
                        childSfCodesForNextLevel.add(childSfcode); // Add for next level recursion

                        // Create Parent -> Child Relationship Cypher
                        String relCypher = String.format(
                                "MATCH (p:`%s` {`%s`: %s}), (c:`%s` {`%s`: %s}) MERGE (p)-[:`%s`]->(c)",
                                hierarchyRel.getFromNodeLabelOrDefault(), eqPkProp, formatValueForCypher(parentSfcode), // Assuming fromNodeLabelOrDefault returns "设备"
                                hierarchyRel.getToNodeLabelOrDefault(),   eqPkProp, formatValueForCypher(childSfcode), // Assuming toNodeLabelOrDefault returns "设备"
                                hierarchyRel.getType()
                        );
                        childCypherBatch.add(relCypher);
                        relsProcessedInBranch++;

                        // Batch execution within loop
                        if (childCypherBatch.size() >= BATCH_SIZE) {
                            executeCypherBatch(session, childCypherBatch, connectionId, "Hierarchy relationships batch for parent " + parentSfcode);
                        }
                    } else {
                        logWarn(connectionId, "Found child for parent SPARNO " + parentIdocid + " with NULL SFCODE.");
                    }
                }
            }

            // Execute remaining items in batch for this level
            if (!childCypherBatch.isEmpty()) {
                executeCypherBatch(session, childCypherBatch, connectionId, "Hierarchy relationships final batch for parent " + parentSfcode);
            }
            logDebug(connectionId, "Created " + relsProcessedInBranch + " hierarchy relationships for direct children of " + parentSfcode);

            // --- RECURSION ---
            for (String childSfcode : childSfCodesForNextLevel) {
                Long childIdocid = sfcodeToIdocidMap.get(childSfcode); // Lookup child IDOCID from map
                if (childIdocid != null) {
                    // Get counts from recursive call
                    Map<String, Long> recursiveCounts = createHierarchyRelationshipsRecursive(
                            session, childIdocid, childSfcode, // Pass child's IDOCID and SFCODE
                            hierarchyRel, sfcodeToIdocidMap, processedHierarchySfCodes,
                            connectionId, eqPkProp,
                            findChildrenBySparnoStmt // Reuse statement
                    );
                    // Add counts from deeper levels
                    relsProcessedInBranch += recursiveCounts.getOrDefault("rels", 0L);
                } else {
                    logWarn(connectionId, "Could not find IDOCID in map for child SFCODE: " + childSfcode + ". Cannot recurse further down this branch.");
                }
            }

        } catch (SQLException e) {
            logError(connectionId, "!!! SQL Error processing hierarchy relationships for children of " + parentSfcode + " (IDOCID: " + parentIdocid + "): " + e.getMessage());
        } catch (Exception e) {
            logError(connectionId, "!!! Unexpected Error processing hierarchy relationships for children of " + parentSfcode + ": " + e.getMessage());
            logger.error("Unexpected Error in hierarchy recursion", e);
        }

        totalCounts.put("rels", relsProcessedInBranch);
        return totalCounts;
    }

    // Helper method in RelationshipMapping or directly used:
    private String getFromNodeLabelOrDefault() { return "设备"; } // Based on your hierarchyRel config
    private String getToNodeLabelOrDefault() { return "设备"; }   // Based on your hierarchyRel config

    private Map<String, String> loadFunpTypeMap(Connection oracleConn, String connectionId) throws Exception {
        Map<String, String> typeMap = new HashMap<>();
        String sql = "SELECT SFTYPE, STYNAME FROM FUNPTYPE";
        logInfo(connectionId, "Loading entity type map from FUNPTYPE...");
        try (Statement stmt = oracleConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String code = rs.getString("SFTYPE");
                String name = rs.getString("STYNAME");
                if (code != null && name != null) {
                    typeMap.put(code.trim(), name.trim());
                } else {
                    logWarn(connectionId, "Found NULL key or value in FUNPTYPE: SFTYPE=" + code + ", STYNAME=" + name);
                }
            }
        } catch (Exception e) {
            logError(connectionId, "!!! Failed to load FUNPTYPE map: " + e.getMessage());
            throw e;
        }
        logInfo(connectionId, "Loaded " + typeMap.size() + " types from FUNPTYPE.");
        return typeMap;
    }

    // --- Main Sync Logic ---
    @Async
    public void executeSync(String connectionId) {
        long startTime = System.currentTimeMillis();
        String currentStep = "Initialization";
        updateStatus(connectionId, "IN_PROGRESS", "Synchronization started.", null);
        logInfo(connectionId, "Synchronization process started for connection " + connectionId);

        // Keep track of processed EQTREE nodes to avoid redundant processing in recursion
        Set<String> processedEqtreeSfCodes = new HashSet<>();

        try (Connection oracleConn = databaseService.getConnection(connectionId);
             Session session = neo4jDriver.session()) {

            currentStep = "Loading Mapping Config";
            logInfo(connectionId, currentStep);
            MappingConfig mappingConfig = mappingConfigLoader.loadFromString(getMappingConfig(connectionId));
            NodeMapping unitMapping = mappingConfig.getNodes().get("unit");
            NodeMapping eqTreeDeviceMapping = mappingConfig.getNodes().get("equipment");
            RelationshipMapping containsRel = mappingConfig.getRelationships().stream()
                    .filter(r -> "包含".equals(r.getType())).findFirst().orElse(null);
            RelationshipMapping hierarchyRel = mappingConfig.getRelationships().stream()
                    .filter(r -> "下级".equals(r.getType())).findFirst().orElse(null); // Find by type name now

            if (unitMapping == null || eqTreeDeviceMapping == null || containsRel == null || hierarchyRel == null) {
                throw new IllegalStateException("Essential mapping definitions (unit, equipment, 包含, 下级) are missing in the YAML config.");
            }
            // Get Neo4j property names once
            String unitPkProp = unitMapping.getProperties().getOrDefault("机组编号", "机组编号");
            String eqPkProp = eqTreeDeviceMapping.getProperties().getOrDefault("位置编号", "位置编号");
            String unitNameProp = unitMapping.getProperties().getOrDefault("名称", "名称");
            String eqNameProp = eqTreeDeviceMapping.getProperties().getOrDefault("名称", "名称");
            String eqAssetProp = eqTreeDeviceMapping.getProperties().getOrDefault("资产编号", "资产编号");

            currentStep = "Loading FUNPTYPE data";
            logInfo(connectionId, currentStep);
            Map<String, String> funpTypeMap = loadFunpTypeMap(oracleConn, connectionId);
            logInfo(connectionId, "Loaded " + funpTypeMap.size() + " entity types from FUNPTYPE.");

            long totalNodesProcessed = 0;
            long totalRelsProcessed = 0;

            // --- Step 1: Process EQJZ - Create Unit, initial Maker/Packer, and relationships ---
            currentStep = "Processing EQJZ data (Units, initial Devices, relationships)";
            logInfo(connectionId, currentStep);
            String eqjzSql = "SELECT INDOCNO, SJZNAME, SFCODE, SFNAME, SPMCODE, SFCODE1, SFNAME1, SPMCODE1 FROM EQJZ";
            logInfo(connectionId, "Reading from Oracle: " + eqjzSql);
            long eqjzRowCount = 0;
            List<String> eqjzCypherBatch = new ArrayList<>();
            List<String> rootEquipmentSfCodes = new ArrayList<>(); // Store SFCODEs for Step 2

            try (Statement stmt = oracleConn.createStatement(); ResultSet rs = stmt.executeQuery(eqjzSql)) {
                while (rs.next()) {
                    eqjzRowCount++;
                    String unitId = rs.getString("INDOCNO");
                    String unitName = rs.getString("SJZNAME");
                    String makerLoc = rs.getString("SFCODE");
                    String makerName = rs.getString("SFNAME");
                    String makerAsset = rs.getString("SPMCODE");
                    String packerLoc = rs.getString("SFCODE1");
                    String packerName = rs.getString("SFNAME1");
                    String packerAsset = rs.getString("SPMCODE1");

                    // 1.1 Create/Merge Unit Node
                    if (unitId != null) {
                        eqjzCypherBatch.add(String.format(
                                "MERGE (u:`%s` {`%s`: %s}) SET u.`%s` = %s",
                                unitMapping.getLabel(), unitPkProp, formatValueForCypher(unitId),
                                unitNameProp, formatValueForCypher(unitName)
                        ));
                        totalNodesProcessed++;
                    } else {
                        logWarn(connectionId, "Skipping Unit creation due to NULL INDOCNO in EQJZ row " + eqjzRowCount);
                        continue;
                    }

                    // 1.2 Create/Merge Maker Node (Initial)
                    if (makerLoc != null) {
                        eqjzCypherBatch.add(String.format(
                                "MERGE (m:`%s` {`%s`: %s}) ON CREATE SET m.`%s` = %s, m.`%s` = %s ON MATCH SET m.`%s` = %s, m.`%s` = %s", // Use ON CREATE/ON MATCH
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(makerLoc),
                                eqNameProp, formatValueForCypher(makerName), eqAssetProp, formatValueForCypher(makerAsset), // ON CREATE
                                eqNameProp, formatValueForCypher(makerName), eqAssetProp, formatValueForCypher(makerAsset)  // ON MATCH (update if needed)
                        ));
                        if (processedEqtreeSfCodes.add(makerLoc)) { // Track processing
                            rootEquipmentSfCodes.add(makerLoc); // Add to list for Step 2
                        }

                        // 1.4 Create Unit -> Maker Relationship
                        eqjzCypherBatch.add(String.format(
                                "MATCH (u:`%s` {`%s`: %s}), (m:`%s` {`%s`: %s}) MERGE (u)-[:`%s`]->(m)",
                                unitMapping.getLabel(), unitPkProp, formatValueForCypher(unitId),
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(makerLoc),
                                containsRel.getType()
                        ));
                        totalRelsProcessed++;
                    } else {
                        logWarn(connectionId, "Skipping Maker creation/relationship for Unit " + unitId + " due to NULL SFCODE.");
                    }

                    // 1.3 Create/Merge Packer Node (Initial)
                    if (packerLoc != null) {
                        eqjzCypherBatch.add(String.format(
                                "MERGE (p:`%s` {`%s`: %s}) ON CREATE SET p.`%s` = %s, p.`%s` = %s ON MATCH SET p.`%s` = %s, p.`%s` = %s",
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(packerLoc),
                                eqNameProp, formatValueForCypher(packerName), eqAssetProp, formatValueForCypher(packerAsset), // ON CREATE
                                eqNameProp, formatValueForCypher(packerName), eqAssetProp, formatValueForCypher(packerAsset)  // ON MATCH
                        ));
                        if (processedEqtreeSfCodes.add(packerLoc)) {
                            rootEquipmentSfCodes.add(packerLoc);
                        }

                        // 1.5 Create Unit -> Packer Relationship
                        eqjzCypherBatch.add(String.format(
                                "MATCH (u:`%s` {`%s`: %s}), (p:`%s` {`%s`: %s}) MERGE (u)-[:`%s`]->(p)",
                                unitMapping.getLabel(), unitPkProp, formatValueForCypher(unitId),
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(packerLoc),
                                containsRel.getType()
                        ));
                        totalRelsProcessed++;
                    } else {
                        logWarn(connectionId, "Skipping Packer creation/relationship for Unit " + unitId + " due to NULL SFCODE1.");
                    }

                    if (eqjzCypherBatch.size() >= BATCH_SIZE) { // Batch execution within loop
                        executeCypherBatch(session, eqjzCypherBatch, connectionId, "EQJZ initial nodes & relationships");
                    }
                }
                // Execute remaining batch
                if (!eqjzCypherBatch.isEmpty()) {
                    executeCypherBatch(session, eqjzCypherBatch, connectionId, "EQJZ initial nodes & relationships (final batch)");
                }
                logInfo(connectionId, "Finished processing " + eqjzRowCount + " rows from EQJZ.");
            }
            logInfo(connectionId, "Initial nodes created/merged from EQJZ: " + totalNodesProcessed); // Log initial node count
            logInfo(connectionId, "Initial relationships created/merged from EQJZ: " + totalRelsProcessed);


            // --- Step 2 & 3: Process EQTREE Children Recursively ---
            currentStep = "Processing EQTREE children recursively";
            logInfo(connectionId, currentStep + " for " + rootEquipmentSfCodes.size() + " root equipment nodes.");

            // Prepare statements for efficiency within recursion
            String findIdocidSql = "SELECT IDOCID FROM EQTREE WHERE SFCODE = ?";
            // Make sure SPARNO column exists and is correctly named in EQTREE
            String findChildrenSql = "SELECT * FROM EQTREE WHERE SPARNO = ?";
            try (PreparedStatement findIdocidStmt = oracleConn.prepareStatement(findIdocidSql);
                 PreparedStatement findChildrenStmt = oracleConn.prepareStatement(findChildrenSql)) {

                for (String rootSfcode : rootEquipmentSfCodes) {
                    // Start recursion for each root maker/packer
                    Map<String, Long> processedCounts = processEqtreeChildrenRecursive(
                            oracleConn, session, rootSfcode, funpTypeMap,
                            eqTreeDeviceMapping, hierarchyRel, processedEqtreeSfCodes,
                            findIdocidStmt, findChildrenStmt, // Pass prepared statements
                            connectionId, eqPkProp // Pass eqPkProp
                    );
                    // Accumulate counts - note: totalNodesProcessed now mainly counts EQTREE updates/creates
                    totalNodesProcessed += processedCounts.getOrDefault("nodes", 0L);
                    totalRelsProcessed += processedCounts.getOrDefault("rels", 0L);
                }

            } catch (SQLException e) {
                // Check if the error is due to missing columns (IDOCID, SPARNO)
                if (e.getMessage().toUpperCase().contains("INVALID IDENTIFIER")) {
                    logError(connectionId, "!!! Critical Error: EQTREE table might be missing required columns 'IDOCID' or 'SPARNO' for recursive processing. " + e.getMessage());
                    throw new SQLException("EQTREE table missing required columns 'IDOCID' or 'SPARNO'.", e);
                } else {
                    logError(connectionId, "!!! Error preparing statements for EQTREE recursion: " + e.getMessage());
                    throw e; // Stop sync if statements can't be prepared
                }
            }

            logInfo(connectionId, "Finished recursive processing of EQTREE.");
            logInfo(connectionId, "Total nodes processed/updated: " + totalNodesProcessed); // Reflects EQTREE processing mainly
            logInfo(connectionId, "Total relationships processed/updated: " + totalRelsProcessed);


            // --- Step 4: Finalization ---
            // Hierarchy relationship (下级) is now created during the recursive step.

            long duration = System.currentTimeMillis() - startTime;
            updateStatus(connectionId, "SUCCESS", "Synchronization completed successfully.", duration);
            logInfo(connectionId, "Synchronization completed successfully in " + duration + "ms.");

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            String errorMessage = "!!! Synchronization failed during step [" + currentStep + "]: " + e.getMessage();
            logger.error(errorMessage, e);
            logError(connectionId, errorMessage);
            updateStatus(connectionId, "FAIL", "Failed during step: " + currentStep + ". Error: " + e.getMessage(), duration);
        }
    }


    /**
     * Recursive function to process children of a given equipment node from EQTREE.
     * ASSUMES EQTREE has IDOCID (PK) and SPARNO (FK to parent IDOCID).
     */
    private Map<String, Long> processEqtreeChildrenRecursive(
            Connection oracleConn, Session session, String parentSfcode, Map<String, String> funpTypeMap,
            NodeMapping eqTreeMapping, RelationshipMapping hierarchyRel, Set<String> processedSfCodes,
            PreparedStatement findIdocidStmt, PreparedStatement findChildrenStmt, // Reusable PreparedStatements
            String connectionId, String eqPkProp) {

        long nodesProcessedInBranch = 0;
        long relsProcessedInBranch = 0;
        Map<String, Long> totalCounts = new HashMap<>();
        totalCounts.put("nodes", 0L);
        totalCounts.put("rels", 0L);

        logDebug(connectionId, "Recursively processing children of: " + parentSfcode);

        Long parentIdocid = null;
        try {
            findIdocidStmt.setString(1, parentSfcode);
            try (ResultSet rsId = findIdocidStmt.executeQuery()) {
                if (rsId.next()) {
                    // Check if IDOCID column exists before trying to get it
                    try {
                        parentIdocid = rsId.getLong("IDOCID"); // Assuming IDOCID is numeric
                        logDebug(connectionId, "Found IDOCID " + parentIdocid + " for parent SFCODE " + parentSfcode);
                    } catch (SQLException idocidEx) {
                        if (idocidEx.getMessage().toUpperCase().contains("INVALID IDENTIFIER")) {
                            logError(connectionId, "!!! Missing 'IDOCID' column in EQTREE, cannot perform recursive lookup via SPARNO. Please add IDOCID and SPARNO columns.");
                            throw new SQLException("Missing 'IDOCID' column in EQTREE for recursion.", idocidEx);
                        } else {
                            throw idocidEx; // Re-throw other SQL errors
                        }
                    }
                } else {
                    logWarn(connectionId, "Could not find IDOCID for parent SFCODE: " + parentSfcode + ". Stopping recursion for this branch.");
                    return totalCounts; // Cannot proceed without parent IDOCID
                }
            }
        } catch (SQLException e) {
            logError(connectionId, "!!! Error finding IDOCID for " + parentSfcode + ": " + e.getMessage());
            return totalCounts; // Stop this branch on error
        }

        List<String> childCypherBatch = new ArrayList<>();
        List<Map<String, Object>> childrenData = new ArrayList<>(); // Store children data for recursion

        try {
            findChildrenStmt.setLong(1, parentIdocid);
            logDebug(connectionId, "Executing query for children: SELECT * FROM EQTREE WHERE SPARNO = " + parentIdocid);
            try (ResultSet rsChildren = findChildrenStmt.executeQuery()) {
                ResultSetMetaData metaData = rsChildren.getMetaData();
                // FIX: Correct variable name from rs to rsChildren
                while (rsChildren.next()) {
                    Map<String, Object> childRowData = resultSetToMap(rsChildren, metaData);
                    childrenData.add(childRowData); // Add for later recursion

                    String childSfcode = (String) childRowData.get("SFCODE".toUpperCase()); // Get child's SFCODE

                    if (childSfcode == null) {
                        logWarn(connectionId, "Found child for parent SPARNO " + parentIdocid + " with NULL SFCODE. Skipping node.");
                        continue;
                    }

                    // Avoid re-processing nodes already handled within this sync run
                    if (processedSfCodes.contains(childSfcode)) {
                        logDebug(connectionId, "Skipping already processed child SFCODE: " + childSfcode);
                        // Still create relationship if it might be missing (MERGE handles duplicates)
                        String relCypher = String.format(
                                "MATCH (p:`%s` {`%s`: %s}), (c:`%s` {`%s`: %s}) MERGE (p)-[:`%s`]->(c)",
                                eqTreeMapping.getLabel(), eqPkProp, formatValueForCypher(parentSfcode),
                                eqTreeMapping.getLabel(), eqPkProp, formatValueForCypher(childSfcode),
                                hierarchyRel.getType()
                        );
                        childCypherBatch.add(relCypher);
                        relsProcessedInBranch++;
                        continue;
                    }
                    processedSfCodes.add(childSfcode); // Mark as processed for this sync run

                    // 1. Create/Update Child Node (using the EQTREE mapping logic)
                    // FIX: Call the correct method name buildNodeMergeQuery
                    String nodeCypher = buildNodeMergeQuery(eqTreeMapping, childRowData, funpTypeMap);
                    if (nodeCypher != null && !nodeCypher.startsWith("-- SKIP")) {
                        childCypherBatch.add(nodeCypher);
                        nodesProcessedInBranch++;
                    } else {
                        processedSfCodes.remove(childSfcode); // Allow reprocessing maybe?
                        logWarn(connectionId, "Node creation skipped for child SFCODE: " + childSfcode + ". Parent-child relationship will not be created.");
                        continue;
                    }

                    // 2. Create Parent -> Child Relationship
                    String relCypher = String.format(
                            "MATCH (p:`%s` {`%s`: %s}), (c:`%s` {`%s`: %s}) MERGE (p)-[:`%s`]->(c)",
                            eqTreeMapping.getLabel(), eqPkProp, formatValueForCypher(parentSfcode),
                            eqTreeMapping.getLabel(), eqPkProp, formatValueForCypher(childSfcode),
                            hierarchyRel.getType()
                    );
                    childCypherBatch.add(relCypher);
                    relsProcessedInBranch++;

                    // Batch execution within loop
                    if (childCypherBatch.size() >= BATCH_SIZE) {
                        executeCypherBatch(session, childCypherBatch, connectionId, "EQTREE children batch for parent " + parentSfcode);
                    }
                }
            } catch (SQLException childrenEx) {
                // Check specifically for SPARNO missing error
                if (childrenEx.getMessage().toUpperCase().contains("INVALID IDENTIFIER") && childrenEx.getMessage().toUpperCase().contains("SPARNO")) {
                    logError(connectionId, "!!! Missing 'SPARNO' column in EQTREE, cannot perform recursive lookup. Please add IDOCID and SPARNO columns.");
                    throw new SQLException("Missing 'SPARNO' column in EQTREE for recursion.", childrenEx);
                } else {
                    throw childrenEx; // Re-throw other SQL errors
                }
            }

            // Execute remaining items in batch for this level
            if (!childCypherBatch.isEmpty()) {
                executeCypherBatch(session, childCypherBatch, connectionId, "EQTREE children final batch for parent " + parentSfcode);
            }
            logDebug(connectionId, "Processed " + nodesProcessedInBranch + " nodes and " + relsProcessedInBranch + " relationships for direct children of " + parentSfcode);

            // --- RECURSION ---
            for (Map<String, Object> childData : childrenData) {
                String childSfcode = (String) childData.get("SFCODE".toUpperCase());
                // Only recurse if the node wasn't skipped due to error/null PK AND exists
                if(childSfcode != null && processedSfCodes.contains(childSfcode)) {
                    // Get counts from recursive call
                    Map<String, Long> recursiveCounts = processEqtreeChildrenRecursive(
                            oracleConn, session, childSfcode, funpTypeMap,
                            eqTreeMapping, hierarchyRel, processedSfCodes,
                            findIdocidStmt, findChildrenStmt,
                            connectionId, eqPkProp
                    );
                    // Add counts from deeper levels
                    nodesProcessedInBranch += recursiveCounts.getOrDefault("nodes", 0L);
                    relsProcessedInBranch += recursiveCounts.getOrDefault("rels", 0L);
                }
            }

        } catch (SQLException e) {
            logError(connectionId, "!!! SQL Error processing children of " + parentSfcode + " (IDOCID: " + (parentIdocid != null ? parentIdocid : "unknown") + "): " + e.getMessage());
        } catch (Exception e) {
            logError(connectionId, "!!! Unexpected Error processing children of " + parentSfcode + ": " + e.getMessage());
            logger.error("Unexpected Error in recursion", e);
        }

        totalCounts.put("nodes", nodesProcessedInBranch);
        totalCounts.put("rels", relsProcessedInBranch);
        return totalCounts;
    }

    private String buildNodeMergeQuery(NodeMapping mapping, Map<String, Object> rowData, Map<String, String> funpTypeMap) {

        String primaryKeyColumn = mapping.getPrimaryKey();
        String nodeKey = mapping.getNodeKey(); // Get the node key to distinguish steps

        // Adjust PK column name based on the source nodeKey for EQJZ packers
        if ("packer_from_eqjz".equals(nodeKey)) {
            primaryKeyColumn = "SFCODE1"; // Use SFCODE1 as the source PK column from EQJZ
        }

        // Determine the Neo4j property name for the primary key
        String primaryKeyPropNeo4j = findPrimaryKeyProp(mapping, primaryKeyColumn);
        if (primaryKeyPropNeo4j == null) {
            primaryKeyPropNeo4j = findPrimaryKeyPropFallback(mapping);
            if (primaryKeyPropNeo4j == null) {
                // If still null, try using the primaryKeyColumn name directly as Neo4j prop name
                logger.warn("Primary key column '{}' not explicitly mapped for nodeKey '{}'. Using original column name '{}' as Neo4j property.",
                        primaryKeyColumn, nodeKey, primaryKeyColumn);
                primaryKeyPropNeo4j = primaryKeyColumn; // Fallback to raw column name
            }
            // Ensure the determined PK property is conceptually part of the properties to set
            if (!mapping.getProperties().containsKey(primaryKeyPropNeo4j)) {
                mapping.getProperties().put(primaryKeyPropNeo4j, primaryKeyColumn);
            }
        }


        Object primaryKeyValue = rowData.get(primaryKeyColumn.toUpperCase()); // Get value using the determined PK column
        if (primaryKeyValue == null) {
            logger.warn("Primary key value for column '{}' is NULL in source table '{}' for nodeKey '{}'. Skipping node.",
                    primaryKeyColumn, mapping.getSource(), nodeKey);
            return "-- SKIP: NULL primary key value for " + primaryKeyColumn;
        }

        String baseLabel = mapping.getLabel(); // Label from YAML (e.g., 设备, 卷接机组)
        String dynamicLabel = null;
        List<String> labelsToSet = new ArrayList<>();
        labelsToSet.add(baseLabel); // Always add the base label

        // Handle dynamic label lookup only for EQTREE data step
        if (mapping.getDynamicLabelLookup() != null && "eqtree_device".equals(nodeKey)) {
            String typeCodeKey = mapping.getDynamicLabelLookup().getKeyColumn().toUpperCase();
            Object typeCode = rowData.get(typeCodeKey);
            if (typeCode != null) {
                dynamicLabel = funpTypeMap.get(typeCode.toString().trim()); // Trim the code before lookup
                if (dynamicLabel != null && !dynamicLabel.isEmpty() && !dynamicLabel.equals(baseLabel)) {
                    labelsToSet.add(dynamicLabel); // Add the dynamic label if found and different
                    logDebug("N/A", "Found dynamic label '" + dynamicLabel + "' for type code '" + typeCode + "'");
                } else if (dynamicLabel == null){
                    logWarn("N/A", "Could not find label in FUNPTYPE for code: '" + typeCode + "' (PK: " + primaryKeyValue + ")");
                }
            } else {
                logWarn("N/A", "Dynamic label key column '" + typeCodeKey + "' is null in EQTREE row with PK " + primaryKeyValue);
            }
        }

        // Build properties string, excluding temporary keys
        String propertiesString = buildPropertiesString(mapping, rowData);

        // Build SET labels string using backticks
        String setLabelsString = buildSetLabelsString(labelsToSet);

        // MERGE using the BASE label and primary key, then SET additional labels and properties
        // Ensure primary key property name also uses backticks
        return String.format("MERGE (n:`%s` {`%s`: %s}) SET n:%s, n += {%s}",
                baseLabel,
                primaryKeyPropNeo4j, // Neo4j property name for PK
                formatValueForCypher(primaryKeyValue),
                setLabelsString, // Set potentially multiple labels
                propertiesString); // Set/update properties
    }

    private String findPrimaryKeyProp(NodeMapping mapping, String primaryKeyColumn) {
        for (Map.Entry<String, String> entry : mapping.getProperties().entrySet()) {
            if (primaryKeyColumn.equalsIgnoreCase(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private String findPrimaryKeyPropFallback(NodeMapping mapping) {
        for (Map.Entry<String, String> entry : mapping.getProperties().entrySet()) {
            if (mapping.getPrimaryKey() != null && mapping.getPrimaryKey().equalsIgnoreCase(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private String buildPropertiesString(NodeMapping mapping, Map<String, Object> rowData) {
        return mapping.getProperties().entrySet().stream()
                .filter(entry -> !entry.getKey().startsWith("_temp_"))
                .map(entry -> {
                    String oracleColName = entry.getValue();
                    String neo4jPropName = entry.getKey();
                    Object value = rowData.get(oracleColName.toUpperCase());
                    if (value == null) {
                        value = rowData.get(oracleColName);
                    }
                    return "`" + neo4jPropName + "`: " + formatValueForCypher(value);
                })
                .collect(Collectors.joining(", "));
    }

    private String buildSetLabelsString(List<String> labelsToSet) {
        return labelsToSet.stream()
                .map(label -> "`" + label + "`")
                .collect(Collectors.joining(":"));
    }

    private String findMappedPropNameNeo4j(NodeMapping nodeMapping, String joinKeyColumnOracle, String connectionId, String nodeKey) {
        for (Map.Entry<String, String> entry : nodeMapping.getProperties().entrySet()) {
            if (joinKeyColumnOracle.equalsIgnoreCase(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void executeCypherBatch(Session session, List<String> queries, String connectionId, String batchDescription) {
        if (queries.isEmpty()) return;
        logInfo(connectionId, "Executing batch (" + queries.size() + " queries) for: " + batchDescription);
        try (Transaction tx = session.beginTransaction()) {
            for (String query : queries) {
                if (query.startsWith("-- SKIP")) {
                    logWarn(connectionId, "Skipped Cypher execution: " + query);
                    continue;
                }
                try {
                    logDebug(connectionId, "Executing Cypher: " + query);
                    tx.run(query);
                } catch (Exception e) {
                    logError(connectionId, "!!! Failed executing Cypher in batch [" + batchDescription + "]: Query='" + query + "' - Error: " + e.getMessage());
                }
            }
            tx.commit();
            logDebug(connectionId, "Batch committed successfully for: " + batchDescription);
        } catch (Neo4jException e) {
            String errorMsg = "!!! Neo4j transaction failed for batch [" + batchDescription + "]: [" + e.code() + "] " + e.getMessage();
            logger.error(errorMsg, e);
            logError(connectionId, errorMsg);
        } catch (Exception e) {
            String errorMsg = "!!! Unexpected error during batch execution [" + batchDescription + "]: " + e.getMessage();
            logger.error(errorMsg, e);
            logError(connectionId, errorMsg);
        } finally {
            queries.clear();
        }
    }

    private Map<String, Object> resultSetToMap(ResultSet rs, ResultSetMetaData metaData) throws Exception {
        int columns = metaData.getColumnCount();
        Map<String, Object> row = new HashMap<>(columns);
        for (int i = 1; i <= columns; ++i) {
            row.put(metaData.getColumnName(i).toUpperCase(), rs.getObject(i));
        }
        return row;
    }

    private String formatValueForCypher(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number) {
            if (value instanceof Double && (Double.isNaN((Double) value) || Double.isInfinite((Double) value))) {
                return "null";
            }
            if (value instanceof Float && (Float.isNaN((Float) value) || Float.isInfinite((Float) value))) {
                return "null";
            }
            return value.toString();
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Timestamp) {
            try {
                return "'" + ((Timestamp) value).toLocalDateTime().toString() + "'";
            } catch (Exception e) {
                logger.warn("Could not format Timestamp: {}. Storing as string.", value);
                String escaped = value.toString().replace("\\", "\\\\").replace("'", "\\'");
                return "'" + escaped + "'";
            }
        }
        if (value instanceof java.sql.Date) {
            try {
                return "'" + ((java.sql.Date) value).toLocalDate().toString() + "'";
            } catch (Exception e) {
                logger.warn("Could not format Date: {}. Storing as string.", value);
                String escaped = value.toString().replace("\\", "\\\\").replace("'", "\\'");
                return "'" + escaped + "'";
            }
        }

        String escaped = value.toString()
                .replace("\\", "\\\\")
                .replace("'", "\\'");
        return "'" + escaped + "'";
    }

    private void updateStatus(String connectionId, String status, String message, Long duration) {
        SyncStatus currentStatus = syncStatusMap.computeIfAbsent(connectionId, k -> new SyncStatus()); // Java 8 computeIfAbsent syntax
        currentStatus.setStatus(status);
        currentStatus.setLastSyncTime(LocalDateTime.now());
        currentStatus.setLastSyncDurationMs(duration);
        currentStatus.addHistory(new SyncStatus.SyncHistory(LocalDateTime.now(), status, message, duration));
        logger.info("Updated status for {}: Status={}, Message='{}', Duration={}", connectionId, status, message, duration);
    }

    public SyncStatus getSyncStatus(String connectionId) {
        SyncStatus status = syncStatusMap.get(connectionId);
        if (status == null) {
            return new SyncStatus();
        }
        return status;
    }

    // --- Logging Helper Methods ---
    private void logInfo(String connectionId, String message) {
        logger.info("[Sync:{}] {}", connectionId, message);
        syncLogService.broadcast(connectionId, "INFO: " + message);
    }
    private void logWarn(String connectionId, String message) {
        logger.warn("[Sync:{}] {}", connectionId, message);
        syncLogService.broadcast(connectionId, "WARN: " + message);
    }
    private void logError(String connectionId, String message) {
        if (!message.contains("!!!")) {
            logger.error("[Sync:{}] {}", connectionId, message);
        }
        syncLogService.broadcast(connectionId, "ERROR: " + message.replace("!!!","").trim());
    }
    private void logDebug(String connectionId, String message) {
        logger.debug("[Sync:{}] {}", connectionId, message);
        // syncLogService.broadcast(connectionId, "DEBUG: " + message); // Keep commented
    }
}

