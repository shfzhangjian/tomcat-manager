package com.zhangjian.tomcatmanager;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhangjian.tomcatmanager.dto.*;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DatabaseService {

    private final ObjectMapper objectMapper;
    private final File connectionsFile = new File("db_connections.json");
    private final File layoutsFile = new File("query_layouts.json");
    private final Map<String, DatabaseConnection> connections = new ConcurrentHashMap<>();
    private final List<QueryLayout> layouts = new ArrayList<>();

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
            "\\b(?:FROM|JOIN)\\s+([a-zA-Z0-9_.\"]+)", Pattern.CASE_INSENSITIVE);


    public DatabaseService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        loadConnections();
        loadLayouts();
    }

    public List<DatabaseConnection> getAllConnections() {
        return new ArrayList<>(connections.values());
    }

    public DatabaseConnection saveConnection(DatabaseConnection connection) throws IOException {
        if (connection.getId() == null || connection.getId().isEmpty()) {
            connection.setId(UUID.randomUUID().toString());
        }
        connections.put(connection.getId(), connection);
        persistConnections();
        return connection;
    }

    public void deleteConnection(String id) throws IOException {
        connections.remove(id);
        persistConnections();
    }

    // 新增方法: 更新同步开关状态
    public void updateSyncEnabled(String connectionId, boolean enabled) throws IOException {
        DatabaseConnection connection = connections.get(connectionId);
        if (connection == null) {
            throw new IllegalArgumentException("Connection not found with id: " + connectionId);
        }
        connection.setSyncEnabled(enabled);
        persistConnections();
    }


    public Connection getConnection(String connectionId) throws SQLException {
        DatabaseConnection dbConn = connections.get(connectionId);
        if (dbConn == null) {
            throw new SQLException("未找到ID为 " + connectionId + " 的连接配置。");
        }
        String url;
        String driverClass;
        Properties props = new Properties();
        props.put("user", dbConn.getUsername());
        props.put("password", dbConn.getPassword());

        if ("mysql".equalsIgnoreCase(dbConn.getType())) {
            driverClass = "com.mysql.cj.jdbc.Driver";
            url = String.format("jdbc:mysql://%s:%d/%s?serverTimezone=UTC&useSSL=false",
                    dbConn.getHost(), dbConn.getPort(), dbConn.getDatabaseName());
        } else if ("oracle".equalsIgnoreCase(dbConn.getType())) {
            driverClass = "oracle.jdbc.OracleDriver";
            url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                    dbConn.getHost(), dbConn.getPort(), dbConn.getDatabaseName());
            props.put("remarksReporting", "true");
        } else {
            throw new SQLException("不支持的数据库类型: " + dbConn.getType());
        }

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC Driver not found: " + driverClass);
        }

        return DriverManager.getConnection(url, props);
    }

    public String testConnection(DatabaseConnection connection) {
        try {
            String tempId = UUID.randomUUID().toString();
            connections.put(tempId, connection);
            try (Connection conn = getConnection(tempId)) {
                return "连接成功！";
            } finally {
                connections.remove(tempId);
            }
        } catch (SQLException e) {
            return "连接失败: " + e.getMessage();
        }
    }

    public List<DatabaseObjectTypeSummary> getDatabaseObjectTypes(String connectionId) throws SQLException {
        List<DatabaseObjectTypeSummary> summaries = new ArrayList<>();
        DatabaseConnection dbConn = connections.get(connectionId);
        if (dbConn == null) throw new SQLException("Connection not found");

        try (Connection conn = getConnection(connectionId)) {
            if ("oracle".equalsIgnoreCase(dbConn.getType())) {
                summaries.add(getOracleObjectCount(conn, "TABLE", "Tables"));
                summaries.add(getOracleObjectCount(conn, "VIEW", "Views"));
                summaries.add(getOracleObjectCount(conn, "SEQUENCE", "Sequences"));
                summaries.add(getOracleObjectCount(conn, "PROCEDURE", "Procedures"));
            } else if ("mysql".equalsIgnoreCase(dbConn.getType())) {
                summaries.add(getMySQLObjectCount(conn, "BASE TABLE", "Tables", "TABLE"));
                summaries.add(getMySQLObjectCount(conn, "VIEW", "Views", "VIEW"));
                summaries.add(getMySQLProcedureCount(conn, "Procedures"));
            }
        }
        return summaries;
    }

    private DatabaseObjectTypeSummary getOracleObjectCount(Connection conn, String objectType, String displayName) throws SQLException {
        long count = 0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM user_objects WHERE object_type = ?")) {
            ps.setString(1, objectType);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) count = rs.getLong(1);
            }
        }
        return new DatabaseObjectTypeSummary(objectType, displayName, count);
    }

    private DatabaseObjectTypeSummary getMySQLObjectCount(Connection conn, String tableType, String displayName, String typeId) throws SQLException {
        long count = 0;
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_type = ?")) {
            ps.setString(1, conn.getCatalog());
            ps.setString(2, tableType);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) count = rs.getLong(1);
            }
        }
        return new DatabaseObjectTypeSummary(typeId, displayName, count);
    }

    private DatabaseObjectTypeSummary getMySQLProcedureCount(Connection conn, String displayName) throws SQLException {
        long count = 0;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'PROCEDURE'")) {
            if (rs.next()) count = rs.getLong(1);
        }
        return new DatabaseObjectTypeSummary("PROCEDURE", displayName, count);
    }

    public PaginatedResult<DatabaseObject> getPaginatedObjects(String connectionId, String type, int page, int size, String filter) throws SQLException {
        List<DatabaseObject> objects = new ArrayList<>();
        long totalElements = 0;
        DatabaseConnection dbConn = connections.get(connectionId);
        if (dbConn == null) throw new SQLException("Connection not found");

        String filterClause = (filter != null && !filter.isEmpty()) ? "%" + filter.toUpperCase() + "%" : null;

        try (Connection conn = getConnection(connectionId)) {
            String countQuery;
            String dataQuery;

            if ("oracle".equalsIgnoreCase(dbConn.getType())) {
                String baseQuery = " FROM user_objects UO LEFT JOIN user_tab_comments ATC ON UO.OBJECT_NAME = ATC.TABLE_NAME WHERE UO.OBJECT_TYPE = ?";
                if(filterClause != null) {
                    baseQuery += " AND (UPPER(UO.OBJECT_NAME) LIKE ? OR ATC.COMMENTS LIKE ?)";
                }
                countQuery = "SELECT count(*)" + baseQuery;
                dataQuery = "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT UO.OBJECT_NAME, ATC.COMMENTS" + baseQuery + " ORDER BY UO.OBJECT_NAME) a WHERE ROWNUM <= ?) WHERE rnum > ?";

            } else { // MySQL
                if ("PROCEDURE".equalsIgnoreCase(type)) {
                    String baseQuery = " FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_type = 'PROCEDURE'";
                    if (filterClause != null) {
                        baseQuery += " AND (UPPER(routine_name) LIKE ? OR UPPER(routine_comment) LIKE ?)";
                    }
                    countQuery = "SELECT count(*)" + baseQuery;
                    dataQuery = "SELECT routine_name, routine_comment" + baseQuery + " ORDER BY routine_name LIMIT ? OFFSET ?";
                } else {
                    String tableType = "TABLE".equalsIgnoreCase(type) ? "BASE TABLE" : "VIEW";
                    String baseQuery = " FROM information_schema.tables WHERE table_schema = ? AND table_type = ?";
                    if(filterClause != null) {
                        baseQuery += " AND (UPPER(table_name) LIKE ? OR UPPER(table_comment) LIKE ?)";
                    }
                    countQuery = "SELECT count(*)" + baseQuery;
                    dataQuery = "SELECT table_name, table_comment" + baseQuery + " ORDER BY table_name LIMIT ? OFFSET ?";
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(countQuery)) {
                int paramIndex = 1;
                if ("oracle".equalsIgnoreCase(dbConn.getType())) {
                    ps.setString(paramIndex++, type);
                    if(filterClause != null) {
                        ps.setString(paramIndex++, filterClause);
                        ps.setString(paramIndex, filterClause);
                    }
                } else {
                    if ("PROCEDURE".equalsIgnoreCase(type)) {
                        if (filterClause != null) {
                            ps.setString(paramIndex++, filterClause);
                            ps.setString(paramIndex, filterClause);
                        }
                    } else {
                        ps.setString(paramIndex++, conn.getCatalog());
                        ps.setString(paramIndex++, "TABLE".equalsIgnoreCase(type) ? "BASE TABLE" : "VIEW");
                        if(filterClause != null) {
                            ps.setString(paramIndex++, filterClause);
                            ps.setString(paramIndex, filterClause);
                        }
                    }
                }
                try(ResultSet rs = ps.executeQuery()) {
                    if(rs.next()) totalElements = rs.getLong(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(dataQuery)) {
                int paramIndex = 1;
                if ("oracle".equalsIgnoreCase(dbConn.getType())) {
                    ps.setString(paramIndex++, type);
                    if(filterClause != null) {
                        ps.setString(paramIndex++, filterClause);
                        ps.setString(paramIndex++, filterClause);
                    }
                    ps.setInt(paramIndex++, (page + 1) * size);
                    ps.setInt(paramIndex, page * size);
                } else {
                    if ("PROCEDURE".equalsIgnoreCase(type)) {
                        if (filterClause != null) {
                            ps.setString(paramIndex++, filterClause);
                            ps.setString(paramIndex++, filterClause);
                        }
                        ps.setInt(paramIndex++, size);
                        ps.setInt(paramIndex, page * size);
                    } else {
                        ps.setString(paramIndex++, conn.getCatalog());
                        ps.setString(paramIndex++, "TABLE".equalsIgnoreCase(type) ? "BASE TABLE" : "VIEW");
                        if(filterClause != null) {
                            ps.setString(paramIndex++, filterClause);
                            ps.setString(paramIndex, filterClause);
                        }
                        ps.setInt(paramIndex++, size);
                        ps.setInt(paramIndex, page * size);
                    }
                }

                try(ResultSet rs = ps.executeQuery()) {
                    while(rs.next()) {
                        objects.add(new DatabaseObject(rs.getString(1), type.toLowerCase(), rs.getString(2)));
                    }
                }
            }
        }
        int totalPages = (int) Math.ceil((double) totalElements / size);
        return new PaginatedResult<>(objects, totalPages, totalElements, page);
    }

    public ObjectDetails getObjectDetails(String connectionId, String objectType, String objectName) throws SQLException {
        ObjectDetails details = new ObjectDetails();
        details.setName(objectName);
        details.setType(objectType);

        DatabaseConnection dbConn = connections.get(connectionId);
        if (dbConn == null) throw new SQLException("Connection not found");

        try (Connection conn = getConnection(connectionId)) {
            boolean isOracle = "oracle".equalsIgnoreCase(dbConn.getType());
            String objectNameForQuery = isOracle ? objectName.toUpperCase() : objectName;

            // 1. Get Columns and Table Comment
            if ("TABLE".equalsIgnoreCase(objectType) || "VIEW".equalsIgnoreCase(objectType)) {
                List<TableColumn> columns = new ArrayList<>();
                if (isOracle) {
                    // Get columns and column comments
                    String colsSql = "SELECT T.COLUMN_NAME, T.DATA_TYPE, T.DATA_LENGTH, C.COMMENTS " +
                            "FROM USER_TAB_COLUMNS T LEFT JOIN USER_COL_COMMENTS C " +
                            "ON T.TABLE_NAME = C.TABLE_NAME AND T.COLUMN_NAME = C.COLUMN_NAME " +
                            "WHERE T.TABLE_NAME = ?";
                    try (PreparedStatement ps = conn.prepareStatement(colsSql)) {
                        ps.setString(1, objectNameForQuery);
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                columns.add(new TableColumn(rs.getString(1), rs.getString(2), rs.getInt(3), rs.getString(4)));
                            }
                        }
                    }
                    // Get table comment
                    String tableCommentSql = "SELECT COMMENTS FROM USER_TAB_COMMENTS WHERE TABLE_NAME = ?";
                    try (PreparedStatement ps = conn.prepareStatement(tableCommentSql)) {
                        ps.setString(1, objectNameForQuery);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) details.setTableComment(rs.getString(1));
                        }
                    }
                } else { // MySQL
                    DatabaseMetaData meta = conn.getMetaData();
                    try (ResultSet rs = meta.getColumns(conn.getCatalog(), null, objectNameForQuery, null)) {
                        while (rs.next()) {
                            columns.add(new TableColumn(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"), rs.getInt("COLUMN_SIZE"), rs.getString("REMARKS")));
                        }
                    }
                    try (ResultSet rs = meta.getTables(conn.getCatalog(), null, objectNameForQuery, null)) {
                        if (rs.next()) details.setTableComment(rs.getString("REMARKS"));
                    }
                }
                details.setColumns(columns);
            }

            // 2. Get DDL
            details.setDdl(getObjectDefinition(connectionId, objectType, objectName));

            // 3. Get Related Objects
            List<RelatedObjectGroup> relatedGroups = new ArrayList<>();
            if ("TABLE".equalsIgnoreCase(objectType)) {
                if(isOracle) {
                    relatedGroups.add(getOracleForeignKeys(conn, objectNameForQuery));
                    relatedGroups.add(getOracleReferencedBy(conn, objectNameForQuery));
                    relatedGroups.add(getOracleDependencies(conn, objectNameForQuery, "VIEW"));
                    relatedGroups.add(getOracleDependencies(conn, objectNameForQuery, "PROCEDURE"));
                    relatedGroups.add(getOracleDependencies(conn, objectNameForQuery, "FUNCTION"));
                } else {
                    // MySQL related object logic can be added here
                }
            }
            details.setRelatedObjectGroups(relatedGroups);
        }
        return details;
    }

    private RelatedObjectGroup getOracleForeignKeys(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT B.TABLE_NAME, B.COLUMN_NAME FROM USER_CONSTRAINTS A " +
                "JOIN USER_CONS_COLUMNS B ON A.R_CONSTRAINT_NAME = B.CONSTRAINT_NAME " +
                "WHERE A.TABLE_NAME = ? AND A.CONSTRAINT_TYPE = 'R'";
        List<DatabaseObject> objects = new ArrayList<>();
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while(rs.next()) {
                    objects.add(new DatabaseObject(rs.getString(1), "table", " references " + rs.getString(2)));
                }
            }
        }
        return new RelatedObjectGroup("外键 (References)", objects);
    }

    private RelatedObjectGroup getOracleReferencedBy(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT A.TABLE_NAME, B.COLUMN_NAME FROM USER_CONSTRAINTS A " +
                "JOIN USER_CONS_COLUMNS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME " +
                "WHERE A.R_CONSTRAINT_NAME IN (SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE TABLE_NAME = ? AND CONSTRAINT_TYPE = 'P')" +
                "AND A.CONSTRAINT_TYPE = 'R'";
        List<DatabaseObject> objects = new ArrayList<>();
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while(rs.next()) {
                    objects.add(new DatabaseObject(rs.getString(1), "table", " on column " + rs.getString(2)));
                }
            }
        }
        return new RelatedObjectGroup("被引用 (Referenced By)", objects);
    }

    private RelatedObjectGroup getOracleDependencies(Connection conn, String tableName, String type) throws SQLException {
        String sql = "SELECT NAME FROM USER_DEPENDENCIES WHERE REFERENCED_NAME = ? AND TYPE = ?";
        List<DatabaseObject> objects = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setString(2, type);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    objects.add(new DatabaseObject(rs.getString(1), type.toLowerCase(), null));
                }
            }
        }
        return new RelatedObjectGroup("依赖" + type, objects);
    }


    private String getObjectDefinition(String connectionId, String objectType, String objectName) throws SQLException {
        DatabaseConnection dbConn = connections.get(connectionId);
        if (dbConn == null) throw new SQLException("Connection not found");

        try (Connection conn = getConnection(connectionId)) {
            if ("mysql".equalsIgnoreCase(dbConn.getType())) {
                String sql = String.format("SHOW CREATE %s `%s`", "VIEW".equalsIgnoreCase(objectType) ? "VIEW" : "TABLE", objectName);
                try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        return rs.getString(2);
                    }
                }
            } else if ("oracle".equalsIgnoreCase(dbConn.getType())) {
                if ("VIEW".equalsIgnoreCase(objectType)) {
                    try (PreparedStatement ps = conn.prepareStatement("SELECT TEXT FROM USER_VIEWS WHERE VIEW_NAME = ?")) {
                        ps.setString(1, objectName);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) return "CREATE OR REPLACE VIEW " + objectName + " AS\n" + rs.getString("TEXT");
                        }
                    }
                } else if ("PROCEDURE".equalsIgnoreCase(objectType) || "FUNCTION".equalsIgnoreCase(objectType)) {
                    StringBuilder ddl = new StringBuilder();
                    try (PreparedStatement ps = conn.prepareStatement("SELECT TEXT FROM USER_SOURCE WHERE NAME = ? ORDER BY LINE")) {
                        ps.setString(1, objectName);
                        try(ResultSet rs = ps.executeQuery()) {
                            while(rs.next()) {
                                ddl.append(rs.getString("TEXT"));
                            }
                        }
                        return ddl.toString();
                    }
                } else if ("SEQUENCE".equalsIgnoreCase(objectType)) {
                    try (PreparedStatement ps = conn.prepareStatement("SELECT LAST_NUMBER FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ?")) {
                        ps.setString(1, objectName);
                        try(ResultSet rs = ps.executeQuery()) {
                            if(rs.next()) return "Next Value: " + rs.getString("LAST_NUMBER");
                        }
                    }
                }
                else { // Assume TABLE
                    // This is a simplified DDL generation, for complex cases a dedicated library might be better
                    StringBuilder ddl = new StringBuilder(String.format("CREATE TABLE \"%s\" (\n", objectName));
                    try (ResultSet columns = conn.getMetaData().getColumns(null, null, objectName, null)) {
                        while (columns.next()) {
                            ddl.append(String.format("    \"%s\" %s(%s)%s,\n",
                                    columns.getString("COLUMN_NAME"),
                                    columns.getString("TYPE_NAME"),
                                    columns.getInt("COLUMN_SIZE"),
                                    "NO".equals(columns.getString("IS_NULLABLE")) ? " NOT NULL" : ""
                            ));
                        }
                    }
                    if (ddl.toString().contains(",")) {
                        ddl.setLength(ddl.length() - 2);
                    }
                    ddl.append("\n);");
                    return ddl.toString();
                }
            }
        }
        return String.format("-- Could not retrieve DDL for %s %s", objectType, objectName);
    }
    // New and Refactored methods start here

    public QueryResult executePaginatedQuery(String connectionId, String sql, int page, int size) {
        if (sql == null || sql.trim().isEmpty()) {
            return QueryResult.error("SQL query cannot be empty.");
        }
        DatabaseConnection dbConn = connections.get(connectionId);
        if (dbConn == null) {
            return QueryResult.error("Connection not found");
        }
        String cleanedSql = sql.trim().replaceAll(";$", "");

        try (Connection conn = getConnection(connectionId)) {
            long totalRows = 0;
            String countQuery = "SELECT COUNT(*) FROM (" + cleanedSql + ")";
            if (!"oracle".equalsIgnoreCase(dbConn.getType())) {
                countQuery += " AS count_alias";
            }
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(countQuery)) {
                if (rs.next()) totalRows = rs.getLong(1);
            }

            int totalPages = (int) Math.ceil((double) totalRows / size);
            String paginatedQuery = buildPaginatedQuery(cleanedSql, page, size, dbConn.getType());

            List<Map<String, Object>> rows = new ArrayList<>();
            List<QueryColumnInfo> columnInfoList;

            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(paginatedQuery)) {
                ResultSetMetaData metaData = rs.getMetaData();
                columnInfoList = getColumnInfoWithComments(conn, cleanedSql, metaData);
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        row.put(metaData.getColumnLabel(i), rs.getObject(i));
                    }
                    rows.add(row);
                }
            }
            return QueryResult.success(columnInfoList, rows, page, totalPages, totalRows);
        } catch (SQLException e) {
            e.printStackTrace();
            return QueryResult.error(e.getMessage());
        }
    }

    private String buildPaginatedQuery(String sql, int page, int size, String dbType) {
        if ("mysql".equalsIgnoreCase(dbType)) {
            return sql + " LIMIT " + size + " OFFSET " + (page * size);
        } else { // Oracle
            return "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (" + sql + ") a WHERE ROWNUM <= " + ((page + 1) * size) + ") WHERE rnum > " + (page * size);
        }
    }

    public Workbook exportQueryToExcel(String connectionId, String sql) throws SQLException, IOException {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query cannot be empty.");
        }
        String cleanedSql = sql.trim().replaceAll(";$", "");
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Query Result");
        try (Connection conn = getConnection(connectionId);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(cleanedSql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            List<QueryColumnInfo> columnInfos = getColumnInfoWithComments(conn, cleanedSql, metaData);
            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < columnInfos.size(); i++) {
                Cell cell = headerRow.createCell(i);
                QueryColumnInfo info = columnInfos.get(i);
                String headerText = info.getName() + (info.getComment() != null && !info.getComment().isEmpty() ? " (" + info.getComment() + ")" : "");
                cell.setCellValue(headerText);
            }
            int rowNum = 1;
            while (rs.next()) {
                Row row = sheet.createRow(rowNum++);
                for (int i = 0; i < columnInfos.size(); i++) {
                    Object value = rs.getObject(columnInfos.get(i).getName());
                    Cell cell = row.createCell(i);
                    if (value != null) {
                        if (value instanceof Number) cell.setCellValue(((Number) value).doubleValue());
                        else if (value instanceof java.util.Date) cell.setCellValue((java.util.Date) value);
                        else cell.setCellValue(value.toString());
                    }
                }
            }
            for (int i = 0; i < columnInfos.size(); i++) sheet.autoSizeColumn(i);
        }
        return workbook;
    }


    private List<QueryColumnInfo> getColumnInfoWithComments(Connection conn, String sql, ResultSetMetaData metaData) throws SQLException {
        Map<String, String> allColumnComments = new HashMap<>();
        DatabaseMetaData dbMetaData = conn.getMetaData();
        boolean isOracle = "Oracle".equalsIgnoreCase(dbMetaData.getDatabaseProductName());
        Matcher matcher = TABLE_NAME_PATTERN.matcher(sql);

        while (matcher.find()) {
            String tableName = matcher.group(1).replace("\"", "");
            if (isOracle) tableName = tableName.toUpperCase();

            try (ResultSet columns = dbMetaData.getColumns(conn.getCatalog(), isOracle ? dbMetaData.getUserName() : conn.getSchema(), tableName, null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME").toUpperCase();
                    String remarks = columns.getString("REMARKS");
                    if (remarks != null && !remarks.isEmpty()) {
                        allColumnComments.putIfAbsent(columnName, remarks);
                    }
                }
            } catch (SQLException e) {
                System.err.println("Could not retrieve metadata for table: " + tableName + ". Error: " + e.getMessage());
            }
        }

        List<QueryColumnInfo> columnInfoList = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnLabel = metaData.getColumnLabel(i);
            String comment = allColumnComments.get(columnLabel.toUpperCase());
            columnInfoList.add(new QueryColumnInfo(columnLabel, comment));
        }
        return columnInfoList;
    }

    public List<QueryLayout> getLayoutsForSql(String sqlHash) {
        synchronized (layouts) {
            List<QueryLayout> result = new ArrayList<>();
            for (QueryLayout l : layouts) {
                if (l.getSqlHash().equals(sqlHash)) {
                    result.add(l);
                }
            }
            return result;
        }
    }

    public QueryLayout saveLayout(QueryLayout layout) throws IOException {
        synchronized (layouts) {
            if (layout.getName() == null) {
                layouts.removeIf(l -> l.getSqlHash().equals(layout.getSqlHash()) && l.getName() == null);
            } else {
                layouts.removeIf(l -> l.getSqlHash().equals(layout.getSqlHash()) && layout.getName().equals(l.getName()));
            }

            if (layout.getId() == null || layout.getId().isEmpty()) {
                layout.setId(UUID.randomUUID().toString());
            }
            layouts.add(layout);
        }
        persistLayouts();
        return layout;
    }

    public void deleteLayout(String layoutId) throws IOException {
        synchronized (layouts) {
            layouts.removeIf(l -> l.getId().equals(layoutId));
        }
        persistLayouts();
    }


    private void loadConnections() {
        if (connectionsFile.exists()) {
            try {
                List<DatabaseConnection> loadedConnections = objectMapper.readValue(connectionsFile, new TypeReference<List<DatabaseConnection>>() {});
                connections.clear();
                for (DatabaseConnection c : loadedConnections) {
                    connections.put(c.getId(), c);
                }
            } catch (IOException e) {
                System.err.println("加载数据库连接文件失败: " + e.getMessage());
            }
        }
    }

    private void persistConnections() throws IOException {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(connectionsFile, new ArrayList<>(connections.values()));
    }

    private void loadLayouts() {
        if (layoutsFile.exists() && layoutsFile.length() > 0) {
            try {
                JsonParser parser = objectMapper.getFactory().createParser(layoutsFile);
                synchronized (layouts) {
                    layouts.clear();
                    JsonToken firstToken = parser.nextToken();

                    if (firstToken == JsonToken.START_ARRAY) {
                        List<QueryLayout> loaded = objectMapper.readValue(parser, new TypeReference<List<QueryLayout>>() {});
                        layouts.addAll(loaded);
                    } else if (firstToken == JsonToken.START_OBJECT) {
                        Map<String, QueryLayout> oldMap = objectMapper.readValue(parser, new TypeReference<Map<String, QueryLayout>>() {});
                        layouts.addAll(oldMap.values());
                        persistLayouts();
                        System.out.println("成功将旧格式的布局文件 (Map) 迁移到新格式 (List)。");
                    }
                }
            } catch (IOException e) {
                System.err.println("加载布局文件失败: " + e.getMessage());
            }
        }
    }

    private void persistLayouts() throws IOException {
        synchronized (layouts) {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(layoutsFile, layouts);
        }
    }
}

