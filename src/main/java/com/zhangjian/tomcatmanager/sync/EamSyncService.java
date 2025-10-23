package com.zhangjian.tomcatmanager.sync;

import com.zhangjian.tomcatmanager.DatabaseService;
import com.zhangjian.tomcatmanager.sync.model.MappingConfig;
import com.zhangjian.tomcatmanager.sync.model.NodeMapping;
import com.zhangjian.tomcatmanager.sync.model.RelationshipMapping;
import com.zhangjian.tomcatmanager.sync.model.SyncStatus;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Result; // 显式导入 Result
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
import java.sql.*; // 导入 PreparedStatement
import java.time.LocalDateTime;
import java.util.*; // 导入 Set 和 HashSet
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.function.Supplier; // 导入 Supplier 用于 orElseGet

/**
 * EAM 到 Neo4j 同步的核心服务。
 * [V2.3] 严格按照用户定义的递归流程重新实现：
 * 1. 预加载 FUNPTYPE。
 * 2. 处理 EQJZ (创建机组、初始设备、机组->设备关系)。
 * 3. *不* 遍历 EQTREE 全表。
 * 4. 从 EQJZ 的根设备开始，递归使用 SPARNO/IDOCID 查找并创建/更新子设备及其关系。
 * 5. [修正] 调整递归逻辑，确保先创建子节点，再创建父->子关系。
 */
@Service
public class EamSyncService {

    private static final Logger logger = LoggerFactory.getLogger(EamSyncService.class);
    private final DatabaseService databaseService;
    private final SyncLogService syncLogService;
    private final MappingConfigLoader mappingConfigLoader;
    private final Driver neo4jDriver;
    private final Map<String, SyncStatus> syncStatusMap = new ConcurrentHashMap<>();

    // Cypher 批处理大小
    private static final int BATCH_SIZE = 500;

    // 默认 YAML 配置模板 (v1.8)
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


    /**
     * 构造函数，通过 Spring 自动注入所需的服务
     */
    @Autowired
    public EamSyncService(DatabaseService databaseService, SyncLogService syncLogService, MappingConfigLoader mappingConfigLoader, Driver neo4jDriver) {
        this.databaseService = databaseService;
        this.syncLogService = syncLogService;
        this.mappingConfigLoader = mappingConfigLoader;
        this.neo4jDriver = neo4jDriver;
    }

    // --- 公共接口方法 ---

    /**
     * [公共接口] 获取指定连接ID的映射配置 (YAML 字符串)
     * @param connectionId 数据库连接ID
     * @return YAML 配置字符串
     * @throws IOException 如果读取文件失败
     */
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

    /**
     * [公共接口] 保存指定连接ID的映射配置 (YAML 字符串)
     * @param connectionId 数据库连接ID
     * @param mappingData YAML 配置字符串
     * @throws IOException 如果写入文件失败
     */
    public void saveMappingConfig(String connectionId, String mappingData) throws IOException {
        Path path = Paths.get("mapping-config-" + connectionId + ".yml");
        Files.write(path, mappingData.getBytes(StandardCharsets.UTF_8));
        logger.info("Saved mapping config to file: {}", path);
    }


    /**
     * 加载 FUNPTYPE 设备类型映射表
     */
    private Map<String, String> loadFunpTypeMap(Connection oracleConn, String connectionId) throws Exception {
        Map<String, String> typeMap = new HashMap<>();
        String sql = "SELECT SFTYPE, STYNAME FROM FUNPTYPE";
        logInfo(connectionId, "Loading entity type map from FUNPTYPE...");
        logInfo(connectionId, "Executing query: " + sql); // 打印 SQL
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


    /**
     * [公共接口] 异步执行完整的同步过程
     * @param connectionId 数据库连接ID
     */
    @Async
    public void executeSync(String connectionId) {
        long startTime = System.currentTimeMillis();
        String currentStep = "Initialization";
        updateStatus(connectionId, "IN_PROGRESS", "Synchronization started.", null);
        logInfo(connectionId, "Synchronization process started for connection " + connectionId);

        // 用于跟踪已处理的 EQTREE 节点 (SFCODE)，防止递归重复处理
        Set<String> processedEqtreeSfCodes = new HashSet<>();
        // Cypher 批处理列表
        List<String> cypherBatch = new ArrayList<>();
        // 存储 SFCODE -> IDOCID 映射，用于递归
        Map<String, Long> sfcodeToIdocidMap = new HashMap<>();

        try (Connection oracleConn = databaseService.getConnection(connectionId);
             Session session = neo4jDriver.session()) {

            // --- 步骤 0: 加载配置和类型映射 ---
            currentStep = "Loading Mapping Config";
            logInfo(connectionId, currentStep);
            MappingConfig mappingConfig = mappingConfigLoader.loadFromString(getMappingConfig(connectionId));

            // 提取需要用到的映射定义
            NodeMapping unitMapping = mappingConfig.getNodes().get("unit");
            NodeMapping eqTreeDeviceMapping = mappingConfig.getNodes().get("equipment");
            RelationshipMapping containsRel = mappingConfig.getRelationships().stream()
                    .filter(r -> "包含".equals(r.getType())).findFirst().orElse(null);
            RelationshipMapping hierarchyRel = mappingConfig.getRelationships().stream()
                    .filter(r -> "下级".equals(r.getType())).findFirst().orElse(null);

            if (unitMapping == null || eqTreeDeviceMapping == null || containsRel == null || hierarchyRel == null) {
                logError(connectionId, "!!! Critical Error: Essential mapping definitions (unit, equipment, 包含, 下级) are missing in the YAML config.");
                throw new IllegalStateException("Essential mapping definitions (unit, equipment, 包含, 下级) are missing in the YAML config.");
            }

            // [修正 v2.2] 提取 Neo4j 属性名 (YAML 中的 *key*)
            final String unitPkProp = findKeyForValue(unitMapping.getProperties(), "INDOCNO", "机组编号");
            final String unitNameProp = findKeyForValue(unitMapping.getProperties(), "SJZNAME", "名称");
            final String eqPkProp = findKeyForValue(eqTreeDeviceMapping.getProperties(), "SFCODE", "位置编号");
            final String eqNameProp = findKeyForValue(eqTreeDeviceMapping.getProperties(), "SFNAME", "名称");
            final String eqAssetProp = findKeyForValue(eqTreeDeviceMapping.getProperties(), "SPMCODE", "资产编号");

            currentStep = "Loading FUNPTYPE data";
            logInfo(connectionId, currentStep);
            Map<String, String> funpTypeMap = loadFunpTypeMap(oracleConn, connectionId);

            long totalNodesProcessed = 0;
            long totalRelsProcessed = 0;

            // --- 步骤 1: 处理 EQJZ - 创建机组、初始设备节点和“包含”关系 ---
            currentStep = "Processing EQJZ data (Units, initial Devices, relationships)";
            logInfo(connectionId, currentStep);
            String eqjzSql = "SELECT INDOCNO, SJZNAME, SFCODE, SFNAME, SPMCODE, SFCODE1, SFNAME1, SPMCODE1 FROM EQJZ";
            logInfo(connectionId, "Reading from Oracle: " + eqjzSql); // 打印 EQJZ SQL
            long eqjzRowCount = 0;
            List<String> rootEquipmentSfCodes = new ArrayList<>(); // 存储根设备(卷接机/包装机)的 SFCODE

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

                    // 1.1 创建/合并 机组 节点
                    if (unitId != null) {
                        cypherBatch.add(String.format(
                                "MERGE (u:`%s` {`%s`: %s}) SET u.`%s` = %s",
                                unitMapping.getLabel(), unitPkProp, formatValueForCypher(unitId),
                                unitNameProp, formatValueForCypher(unitName)
                        ));
                        totalNodesProcessed++;
                    } else {
                        logWarn(connectionId, "Skipping Unit creation due to NULL INDOCNO in EQJZ row " + eqjzRowCount);
                        continue;
                    }

                    // 1.2 创建/合并 卷接机 节点 (初始信息)
                    if (makerLoc != null) {
                        cypherBatch.add(String.format(
                                "MERGE (m:`%s` {`%s`: %s}) ON CREATE SET m.`%s` = %s, m.`%s` = %s ON MATCH SET m.`%s` = %s, m.`%s` = %s",
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(makerLoc),
                                eqNameProp, formatValueForCypher(makerName), eqAssetProp, formatValueForCypher(makerAsset), // ON CREATE
                                eqNameProp, formatValueForCypher(makerName), eqAssetProp, formatValueForCypher(makerAsset)  // ON MATCH
                        ));
                        if (!rootEquipmentSfCodes.contains(makerLoc)) {
                            rootEquipmentSfCodes.add(makerLoc); // 添加到根设备列表
                        }
                        totalNodesProcessed++; // 计入节点处理

                        // 1.4 创建 机组 -> 卷接机 的 :包含 关系
                        cypherBatch.add(String.format(
                                "MATCH (u:`%s` {`%s`: %s}), (m:`%s` {`%s`: %s}) MERGE (u)-[:`%s`]->(m)",
                                unitMapping.getLabel(), unitPkProp, formatValueForCypher(unitId),
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(makerLoc),
                                containsRel.getType()
                        ));
                        totalRelsProcessed++;
                    } else {
                        logWarn(connectionId, "Skipping Maker creation/relationship for Unit " + unitId + " due to NULL SFCODE.");
                    }

                    // 1.3 创建/合并 包装机 节点 (初始信息)
                    if (packerLoc != null) {
                        cypherBatch.add(String.format(
                                "MERGE (p:`%s` {`%s`: %s}) ON CREATE SET p.`%s` = %s, p.`%s` = %s ON MATCH SET p.`%s` = %s, p.`%s` = %s",
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(packerLoc),
                                eqNameProp, formatValueForCypher(packerName), eqAssetProp, formatValueForCypher(packerAsset), // ON CREATE
                                eqNameProp, formatValueForCypher(packerName), eqAssetProp, formatValueForCypher(packerAsset)  // ON MATCH
                        ));
                        if (!rootEquipmentSfCodes.contains(packerLoc)) {
                            rootEquipmentSfCodes.add(packerLoc);
                        }
                        totalNodesProcessed++; // 计入节点处理

                        // 1.5 创建 机组 -> 包装机 的 :包含 关系
                        cypherBatch.add(String.format(
                                "MATCH (u:`%s` {`%s`: %s}), (p:`%s` {`%s`: %s}) MERGE (u)-[:`%s`]->(p)",
                                unitMapping.getLabel(), unitPkProp, formatValueForCypher(unitId),
                                eqTreeDeviceMapping.getLabel(), eqPkProp, formatValueForCypher(packerLoc),
                                containsRel.getType()
                        ));
                        totalRelsProcessed++;
                    } else {
                        logWarn(connectionId, "Skipping Packer creation/relationship for Unit " + unitId + " due to NULL SFCODE1.");
                    }

                    if (cypherBatch.size() >= BATCH_SIZE) {
                        executeCypherBatch(session, cypherBatch, connectionId, "EQJZ initial nodes & relationships");
                    }
                }
                if (!cypherBatch.isEmpty()) {
                    executeCypherBatch(session, cypherBatch, connectionId, "EQJZ initial nodes & relationships (final batch)");
                }
                logInfo(connectionId, "Finished processing " + eqjzRowCount + " rows from EQJZ.");
            }
            logInfo(connectionId, "Nodes processed after EQJZ step: " + totalNodesProcessed);
            logInfo(connectionId, "Relationships processed after EQJZ step: " + totalRelsProcessed);


            // --- 步骤 2: 递归处理 EQTREE 子设备 ---
            currentStep = "Processing EQTREE nodes and hierarchy recursively";
            logInfo(connectionId, currentStep + " starting from " + rootEquipmentSfCodes.size() + " root equipment nodes.");

            // 准备 SQL 语句
            String findNodeBySfcodeSql = "SELECT * FROM EQTREE WHERE SFCODE = ?";
            String findChildrenBySparnoSql = "SELECT * FROM EQTREE WHERE SPARNO = ?";

            try (PreparedStatement findNodeStmt = oracleConn.prepareStatement(findNodeBySfcodeSql);
                 PreparedStatement findChildrenStmt = oracleConn.prepareStatement(findChildrenBySparnoSql)) {

                // 检查 EQTREE 是否包含必要列
                checkEqtreeSchema(oracleConn, connectionId);

                for (String rootSfcode : rootEquipmentSfCodes) {
                    Map<String, Long> processedCounts = processEqtreeNodeAndChildrenRecursive(
                            session, cypherBatch, oracleConn,
                            rootSfcode, // 根设备的 SFCODE
                            eqTreeDeviceMapping, funpTypeMap, eqPkProp, hierarchyRel,
                            processedEqtreeSfCodes, // 跟踪已处理的 SFCODE
                            findNodeStmt, findChildrenStmt, connectionId
                    );
                    totalNodesProcessed += processedCounts.getOrDefault("nodes", 0L);
                    totalRelsProcessed += processedCounts.getOrDefault("rels", 0L);
                }

                // 执行递归中剩余的最后批处理
                if (!cypherBatch.isEmpty()) {
                    executeCypherBatch(session, cypherBatch, connectionId, "EQTREE recursive processing (final batch)");
                }

            } catch (SQLException e) {
                logError(connectionId, "!!! Error preparing statements for EQTREE recursion: " + e.getMessage());
                throw e;
            }

            logInfo(connectionId, "Finished recursive processing of EQTREE.");
            logInfo(connectionId, "Total nodes processed/updated (cumulative): " + totalNodesProcessed);
            logInfo(connectionId, "Total relationships processed/updated (cumulative): " + totalRelsProcessed);


            // --- 步骤 3: 最终化 ---
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
     * 检查 EQTREE 是否包含 IDOCID 和 SPARNO
     */
    private void checkEqtreeSchema(Connection oracleConn, String connectionId) throws SQLException {
        try (Statement stmt = oracleConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM EQTREE WHERE ROWNUM = 1")) {
            ResultSetMetaData metaData = rs.getMetaData();
            boolean hasIdocid = false;
            boolean hasSparno = false;
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if ("IDOCID".equalsIgnoreCase(metaData.getColumnName(i))) hasIdocid = true;
                if ("SPARNO".equalsIgnoreCase(metaData.getColumnName(i))) hasSparno = true;
            }
            if (!hasIdocid || !hasSparno) {
                logError(connectionId, "!!! Critical Error: EQTREE table missing required columns 'IDOCID' or 'SPARNO' for recursive processing.");
                throw new SQLException("EQTREE table missing required columns 'IDOCID' or 'SPARNO'.");
            }
            logInfo(connectionId, "EQTREE schema check passed (IDOCID and SPARNO found).");
        }
    }


    /**
     * [V2.3 递归方法 - 修正关系创建顺序]
     * 1. 检查节点是否已处理。
     * 2. 查找并更新当前节点 (parentSfcode) 的属性和标签。
     * 3. 查找当前节点的子节点 (通过 IDOCID -> SPARNO)。
     * 4. [修正] *先* 递归调用自身处理*子*节点。
     * 5. [修正] *后* 创建 (父 -> 子) 的 :下级 关系。
     */
    private Map<String, Long> processEqtreeNodeAndChildrenRecursive(
            Session session, List<String> cypherBatch, Connection oracleConn,
            String parentSfcode, // 当前正在处理的节点的 SFCODE
            NodeMapping eqTreeMapping, Map<String, String> funpTypeMap,
            String eqPkProp, RelationshipMapping hierarchyRel,
            Set<String> processedSfCodes, // 跟踪已处理的 SFCODE
            PreparedStatement findNodeStmt, // SELECT * FROM EQTREE WHERE SFCODE = ?
            PreparedStatement findChildrenStmt, // SELECT * FROM EQTREE WHERE SPARNO = ?
            String connectionId
    ) {
        long nodesProcessedInBranch = 0;
        long relsProcessedInBranch = 0;
        Map<String, Long> totalCounts = new HashMap<>();
        totalCounts.put("nodes", 0L);
        totalCounts.put("rels", 0L);

        // 1. 检查是否已处理过
        if (parentSfcode == null || !processedSfCodes.add(parentSfcode)) {
            if (parentSfcode != null) {
                logDebug(connectionId, "Skipping hierarchy processing for already processed node: " + parentSfcode);
            }
            return totalCounts; // 如果 SFCODE 为空或已处理，则停止
        }

        logDebug(connectionId, "Recursively processing node: " + parentSfcode);

        Map<String, Object> parentRowData = null;
        Long parentIdocid = null;

        // 2. 查找并更新当前节点
        try {
            findNodeStmt.setString(1, parentSfcode);
            logInfo(connectionId, "Executing query: SELECT * FROM EQTREE WHERE SFCODE = ? (param: " + parentSfcode + ")"); // 打印 SQL
            try (ResultSet rsNode = findNodeStmt.executeQuery()) {
                if (rsNode.next()) {
                    parentRowData = resultSetToMap(rsNode, rsNode.getMetaData());
                    Object idocidObj = parentRowData.get("IDOCID".toUpperCase());
                    if (idocidObj instanceof Number) {
                        parentIdocid = ((Number) idocidObj).longValue();
                    }

                    // 2a. 更新节点属性和动态标签
                    String nodeCypher = buildEqTreeNodeMergeQuery(eqTreeMapping, parentRowData, funpTypeMap, eqPkProp, connectionId); // 传入 connectionId
                    if (nodeCypher != null && !nodeCypher.startsWith("-- SKIP")) {
                        cypherBatch.add(nodeCypher);
                        nodesProcessedInBranch++;
                        checkBatch(session, cypherBatch, connectionId, "EQTREE node update (recursive)");
                    } else {
                        logWarn(connectionId, "Node merge skipped for SFCODE: " + parentSfcode + ". Stopping recursion for this branch.");
                        return totalCounts; // 停止
                    }
                } else {
                    logWarn(connectionId, "Could not find EQTREE entry for SFCODE: " + parentSfcode + ". This node (previously created from EQJZ?) might be missing from EQTREE or SFCODE mismatch. Stopping recursion for this branch.");
                    return totalCounts; // 停止
                }
            }
        } catch (Exception e) {
            logError(connectionId, "!!! Error finding/updating node for SFCODE " + parentSfcode + ": " + e.getMessage());
            return totalCounts; // 停止
        }

        // 3. 查找子节点
        if (parentIdocid == null) {
            logWarn(connectionId, "Could not find IDOCID for parent SFCODE: " + parentSfcode + ". Cannot find children.");
            return totalCounts;
        }

        List<Map<String, Object>> childrenData = new ArrayList<>(); // 存储子节点数据用于递归

        try {
            findChildrenStmt.setLong(1, parentIdocid);
            logInfo(connectionId, "Executing query for children: SELECT * FROM EQTREE WHERE SPARNO = " + parentIdocid); // 打印 SQL
            try (ResultSet rsChildren = findChildrenStmt.executeQuery()) {
                ResultSetMetaData metaData = rsChildren.getMetaData();
                while (rsChildren.next()) {
                    childrenData.add(resultSetToMap(rsChildren, metaData)); // 存储行数据
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            logError(connectionId, "!!! SQL Error processing children of " + parentSfcode + " (IDOCID: " + parentIdocid + "): " + e.getMessage());
            return totalCounts; // 停止此分支
        }

        // 4. [修正] 处理子节点 (先递归，后创建关系)
        logDebug(connectionId, "Found " + childrenData.size() + " children for " + parentSfcode);
        for (Map<String, Object> childRowData : childrenData) {
            String childSfcode = (String) childRowData.get("SFCODE".toUpperCase());
            if (childSfcode == null) {
                logWarn(connectionId, "Found child for parent SPARNO " + parentIdocid + " with NULL SFCODE. Skipping relationship.");
                continue;
            }

            // 4b. [修正] 先递归调用
            Map<String, Long> recursiveCounts = processEqtreeNodeAndChildrenRecursive(
                    session, cypherBatch, oracleConn,
                    childSfcode, // 传入子节点的 SFCODE
                    eqTreeMapping, funpTypeMap, eqPkProp, hierarchyRel,
                    processedSfCodes,
                    findNodeStmt, findChildrenStmt, connectionId
            );
            // 累加所有下级分支处理的节点和关系数量
            nodesProcessedInBranch += recursiveCounts.getOrDefault("nodes", 0L);
            relsProcessedInBranch += recursiveCounts.getOrDefault("rels", 0L);

            // 4a. [修正] 后创建 父 -> 子 关系 (此时子节点必定已创建/更新)
            String relCypher = String.format(
                    "MATCH (p:`%s` {`%s`: %s}), (c:`%s` {`%s`: %s}) MERGE (p)-[:`%s`]->(c)",
                    eqTreeMapping.getLabel(), eqPkProp, formatValueForCypher(parentSfcode), // 父
                    eqTreeMapping.getLabel(), eqPkProp, formatValueForCypher(childSfcode),  // 子
                    hierarchyRel.getType() // :下级
            );
            cypherBatch.add(relCypher);
            relsProcessedInBranch++; // 计入当前层的关系
            checkBatch(session, cypherBatch, connectionId, "Hierarchy relationships batch for parent " + parentSfcode);
        }

        totalCounts.put("nodes", nodesProcessedInBranch);
        totalCounts.put("rels", relsProcessedInBranch);
        return totalCounts;
    }


    /**
     * 检查批处理列表是否已满，如果已满则执行
     */
    private void checkBatch(Session session, List<String> queries, String connectionId, String batchDescription) {
        if (queries.size() >= BATCH_SIZE) {
            logInfo(connectionId, "Batch limit reached (" + BATCH_SIZE + "). Executing batch for: " + batchDescription);
            executeCypherBatch(session, queries, connectionId, batchDescription);
        }
    }

    /**
     * 构建用于 EQTREE 节点的 Cypher MERGE 查询。
     * 此方法负责合并节点、设置完整属性并添加动态标签。
     * @param mapping `equipment` 节点的映射定义
     * @param rowData 从 EQTREE 读取的单行数据
     * @param funpTypeMap SFTYPE -> STYNAME 的映射
     * @param primaryKeyPropNeo4j Neo4j 中用作主键的属性名 (例如 "位置编号")
     * @param connectionId 仅用于日志记录
     * @return 生成的 Cypher 查询字符串
     */
    private String buildEqTreeNodeMergeQuery(NodeMapping mapping, Map<String, Object> rowData, Map<String, String> funpTypeMap, String primaryKeyPropNeo4j, String connectionId) {
        String primaryKeyColumn = findOracleColumnName(mapping.getProperties(), primaryKeyPropNeo4j, "SFCODE");
        if(primaryKeyColumn == null) {
            primaryKeyColumn = "SFCODE";
            logger.warn("Could not find Oracle column for Neo4j property '{}' in mapping. Falling back to '{}'.", primaryKeyPropNeo4j, primaryKeyColumn);
        }

        Object primaryKeyValue = rowData.get(primaryKeyColumn.toUpperCase());
        if (primaryKeyValue == null) {
            logger.warn("Primary key value for column '{}' (mapped from '{}') is NULL in EQTREE. Skipping node.", primaryKeyColumn, primaryKeyPropNeo4j);
            return "-- SKIP: NULL primary key value for " + primaryKeyColumn;
        }

        String baseLabel = mapping.getLabel(); // "设备"
        List<String> labelsToSet = new ArrayList<>();
        labelsToSet.add(baseLabel);

        // 动态标签查找
        if (mapping.getDynamicLabelLookup() != null) {
            String typeCodeKey = mapping.getDynamicLabelLookup().getKeyColumn().toUpperCase();
            Object typeCode = rowData.get(typeCodeKey); // e.g., rowData.get("SFTYPE")
            if (typeCode != null) {
                String dynamicLabel = funpTypeMap.get(typeCode.toString().trim());
                if (dynamicLabel != null && !dynamicLabel.isEmpty() && !dynamicLabel.equals(baseLabel)) {
                    labelsToSet.add(dynamicLabel);
                } else if (dynamicLabel == null){
                    logWarn(connectionId, "Could not find label in FUNPTYPE for code: '" + typeCode + "' (PK: " + primaryKeyValue + ")");
                }
            } else {
                logWarn(connectionId, "Dynamic label key column '" + typeCodeKey + "' is null in EQTREE row with PK " + primaryKeyValue);
            }
        }

        // 构建属性 SET 字符串
        String propertiesString = buildPropertiesString(mapping, rowData);
        // 构建标签 SET 字符串
        String setLabelsString = buildSetLabelsString(labelsToSet);

        // MERGE 确保节点存在，SET 确保所有标签和属性被更新
        return String.format("MERGE (n:`%s` {`%s`: %s}) SET n:%s, n += {%s}",
                baseLabel,
                primaryKeyPropNeo4j,
                formatValueForCypher(primaryKeyValue),
                setLabelsString,
                propertiesString);
    }

    // --- 辅助方法 (查找、构建字符串) ---

    /**
     * [v2.2 新增] 辅助方法：根据 Oracle 列名 (value) 查找 Neo4j 属性名 (key)
     */
    private String findKeyForValue(Map<String, String> properties, String oracleValue, String fallbackKey) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (oracleValue.equalsIgnoreCase(entry.getValue())) {
                return entry.getKey(); // 找到了，返回 Neo4j 属性名 (Key)
            }
        }
        logger.warn("Could not find mapped Neo4j property for Oracle column '{}'. Falling back to '{}'.", oracleValue, fallbackKey);
        return fallbackKey;
    }

    /**
     * [v2.2 新增] 根据 Neo4j 属性名(key) 查找 Oracle 列名(value)
     */
    private String findOracleColumnName(Map<String, String> properties, String neo4jKey, String fallback) {
        String oracleCol = properties.get(neo4jKey);
        return oracleCol != null ? oracleCol : fallback;
    }


    /**
     * 构建 Neo4j 属性 SET 字符串
     */
    private String buildPropertiesString(NodeMapping mapping, Map<String, Object> rowData) {
        // 使用 Java 8 兼容的 stream/collect
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

    /**
     * 构建 Neo4j 标签 SET 字符串
     */
    private String buildSetLabelsString(List<String> labelsToSet) {
        // 使用 Java 8 兼容的 stream/collect
        return labelsToSet.stream()
                .map(label -> "`" + label + "`")
                .collect(Collectors.joining(":"));
    }

    /**
     * 查找 EQTREE 属性映射中对应的 Oracle 列名
     */
    private String findMappedPropNameNeo4j(NodeMapping nodeMapping, String joinKeyColumnOracle, String connectionId, String nodeKey) {
        for (Map.Entry<String, String> entry : nodeMapping.getProperties().entrySet()) {
            if (joinKeyColumnOracle.equalsIgnoreCase(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * 执行 Cypher 批处理
     */
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
                    // [修正 v2.3] 将 logDebug 提升为 logInfo 以打印 Cypher
                    logInfo(connectionId, "Executing Cypher: " + query);
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

    /**
     * 将 JDBC ResultSet 行转换为 Map
     */
    private Map<String, Object> resultSetToMap(ResultSet rs, ResultSetMetaData metaData) throws Exception {
        int columns = metaData.getColumnCount();
        Map<String, Object> row = new HashMap<>(columns);
        for (int i = 1; i <= columns; ++i) {
            row.put(metaData.getColumnName(i).toUpperCase(), rs.getObject(i));
        }
        return row;
    }

    /**
     * 将 Java 对象格式化为 Cypher 字符串值
     */
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
        if (value instanceof java.sql.Timestamp) {
            try {
                return "'" + ((java.sql.Timestamp) value).toLocalDateTime().toString() + "'";
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

        // 默认按字符串处理
        String escaped = value.toString()
                .replace("\\", "\\\\") // 必须先转义反斜杠
                .replace("'", "\\'"); // 再转义单引号
        return "'" + escaped + "'";
    }

    /**
     * 更新同步状态
     */
    private void updateStatus(String connectionId, String status, String message, Long duration) {
        // 使用 Java 8 兼容的 computeIfAbsent
        SyncStatus currentStatus = syncStatusMap.computeIfAbsent(connectionId, k -> new SyncStatus());

        currentStatus.setStatus(status);
        currentStatus.setLastSyncTime(LocalDateTime.now());
        currentStatus.setLastSyncDurationMs(duration);
        currentStatus.addHistory(new SyncStatus.SyncHistory(LocalDateTime.now(), status, message, duration));
        logger.info("Updated status for {}: Status={}, Message='{}', Duration={}", connectionId, status, message, duration);
    }

    /**
     * [公共接口] 获取指定连接的同步状态
     */
    public SyncStatus getSyncStatus(String connectionId) {
        SyncStatus status = syncStatusMap.get(connectionId);
        if (status == null) {
            return new SyncStatus(); // 返回一个默认的空状态
        }
        return status;
    }

    // --- 日志辅助方法 ---
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
        // syncLogService.broadcast(connectionId, "DEBUG: " + message); // 保持注释以避免性能问题
    }
}

