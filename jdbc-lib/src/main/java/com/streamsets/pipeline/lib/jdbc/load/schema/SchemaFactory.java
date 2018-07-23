package com.streamsets.pipeline.lib.jdbc.load.schema;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.CDCTableConfigBean;
import com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class SchemaFactory {

    private final static Logger LOG = LoggerFactory.getLogger(SchemaFactory.class);

    private final static String TABLECONFIGBEAN = "com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean";
    private final static String SCHEMATABLECONFIGBEAN = "com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean";
    private final static String CDC_TABLE_CONFIG_BEAN = "com.streamsets.pipeline.stage.origin.jdbc.cdc.sqlserver.CDCTableConfigBean";

    final static String TABLE_NAME = "TABLE_NAME";
    final static String COLUMN_NAME = "COLUMN_NAME";
    final static String TYPE_NAME = "TYPE_NAME";
    final static String PRI = "PRI";
    final static String KEY_SEQ = "KEY_SEQ";
    final static String PK_TABLE_NAME = "PKTABLE_NAME";
    final static String PK_COLUMN_NAME = "PKCOLUMN_NAME";
    final static String FK_COLUMN_NAME = "FKCOLUMN_NAME";

    private final static String ERROR_PREFIX = "EDITMAPPING_ERROR:";
    private final static String LOAD_SCHEMA_ERROR = ERROR_PREFIX + "Load %s schema error: %s";
    private final static String UNSUPPORT_DATABASE = ERROR_PREFIX + "%s is unsupport for now";
    private final static String CONN_ERROR = ERROR_PREFIX + "Try connect to database failed,please check your jdbc configure";
    private final static String TABLECONFIG_EMPTY_ERROR = ERROR_PREFIX + "Table config cannot be empty";

    private static boolean is_mssql_cdc = false;


    public static List<SchemaBean> getSchemaConfigs(List<?> tableConfigs) {
        List<SchemaBean> schemaConfigBeans = new ArrayList<>();

        if (null != tableConfigs && CollectionUtils.isNotEmpty(tableConfigs)) {
            Object firstField = tableConfigs.get(0);

            if (firstField != null) {
                String typeName = firstField.getClass().getTypeName();

                if (StringUtils.isNotBlank(typeName)) {
                    if (typeName.equals(TABLECONFIGBEAN)) {
                        // oracle cdc
                        for (Object field : tableConfigs) {
                            SchemaBean schemaBean = new SchemaBean();

                            schemaBean.setSchema(((TableConfigBean) field).schema);
                            schemaBean.setTablePattern(((TableConfigBean) field).tablePattern);
                            schemaBean.setTableExceludePattern(((TableConfigBean) field).tableExclusionPattern);

                            schemaConfigBeans.add(schemaBean);
                        }
                    } else if (typeName.equals(SCHEMATABLECONFIGBEAN)) {
                        // jdbc multitable
                        for (Object field : tableConfigs) {
                            SchemaBean schemaBean = new SchemaBean();

                            schemaBean.setSchema(((SchemaTableConfigBean) field).schema);
                            schemaBean.setTablePattern(((SchemaTableConfigBean) field).table);
                            schemaBean.setTableExceludePattern(((SchemaTableConfigBean) field).excludePattern);

                            schemaConfigBeans.add(schemaBean);
                        }
                    } else if (typeName.equals(CDC_TABLE_CONFIG_BEAN)) {
                        // sql server cdc
                        for (Object field : tableConfigs) {
                            SchemaBean schemaBean = new SchemaBean();

                            schemaBean.setSchema("cdc");
                            schemaBean.setTablePattern(((CDCTableConfigBean) field).capture_instance);
                            schemaBean.setTableExceludePattern(((CDCTableConfigBean) field).tableExclusionPattern);

                            schemaConfigBeans.add(schemaBean);
                            is_mssql_cdc = true;
                        }
                    }
                }
            }
        }

        return schemaConfigBeans;
    }

    public List<RelateDataBaseTable> loadSchemaList(HikariPoolConfigBean hikariConfigBean, List<?> tableConfigs) {
        List<RelateDataBaseTable> relateDataBaseTables;
        List<SchemaBean> schemaBeans = getSchemaConfigs(tableConfigs);
        String connectionString = hikariConfigBean.connectionString;
        ISchemaValidator iSchemaValidator = null;
        HikariDataSource dataSource = null;
        Connection conn = null;
        Statement statement = null;
        String dbName = "";

        if (CollectionUtils.isEmpty(schemaBeans)) {
            throw new RuntimeException(TABLECONFIG_EMPTY_ERROR);
        }

        try {
            try {
                dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
                conn = dataSource.getConnection();
            } catch (StageException e) {
                throw new RuntimeException(ERROR_PREFIX + JdbcErrors.JDBC_00.getMessage(), e);
            } catch (SQLException e) {
                throw new RuntimeException(ERROR_PREFIX + JdbcErrors.JDBC_06.getMessage(), e);
            }

            // instantiation interface iSchemaValidator
            if (dataSource != null && conn != null) {
                if (connectionString.startsWith(DatabaseType.ORACLE.getType())) {
                    iSchemaValidator = new OracleSchemaValidator();
                    dbName = "oracle";
                } else if (connectionString.startsWith(DatabaseType.MYSQL.getType())) {
                    iSchemaValidator = new MysqlSchemaValidator();
                    dbName = "mysql";
                } else if (connectionString.startsWith(DatabaseType.SQLSERVER.getType())) {
                    iSchemaValidator = new SqlserverSchemaValidator();
                    dbName = "sql server";
                }

                if (iSchemaValidator != null) {
                    try {
                        relateDataBaseTables = iSchemaValidator.validateSchema(conn, schemaBeans);

                        if (relateDataBaseTables == null) {
                            throw new RuntimeException(String.format(LOAD_SCHEMA_ERROR, dbName, "Missing schema or table pattern"));
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(String.format(LOAD_SCHEMA_ERROR, dbName, e.toString()), e);
                    }
                } else {
                    throw new RuntimeException(String.format(UNSUPPORT_DATABASE, hikariConfigBean.connectionString));
                }
            } else {
                throw new RuntimeException(CONN_ERROR);
            }
        } finally {
            JdbcUtil.closeQuietly(conn);
            JdbcUtil.closeQuietly(dataSource);
            JdbcUtil.closeQuietly(statement);
        }

        return relateDataBaseTables;
    }

    public String loadSchemaJson(HikariPoolConfigBean hikariConfigBean, List<?> tableConfigs) {
        ObjectMapper mapper = new ObjectMapper();
        String tableSchemasJson = new String();

        List<RelateDataBaseTable> tableSchemas = this.loadSchemaList(hikariConfigBean, tableConfigs);

        if (tableSchemas != null && CollectionUtils.isNotEmpty(tableSchemas)) {
            try {
                // list to json
                tableSchemasJson = mapper.writeValueAsString(tableSchemas);
            } catch (IOException e) {
                throw new RuntimeException(ERROR_PREFIX + String.format("Parse schema list to json failed: %s", e.toString()), e);
            }
        } else {
            throw new RuntimeException(ERROR_PREFIX + String.format("Load schema list failed"));
        }


        return tableSchemasJson;
    }

    static void setPrimaryKey(RelateDatabaseField field, Map<String, Integer> map) {
        if (field != null && map != null) {
            if (map.containsKey(field.getField_name())) {
                field.setKey(PRI);
                field.setPrimary_key_position(map.get(field.getField_name()));
            }
        }
    }

    static void setForeignKey(RelateDatabaseField field, Map<String, SchemaBean> map) {
        if (field != null && map != null) {
            if (map.containsKey(field.getField_name())) {
                SchemaBean schemaBean = map.get(field.getField_name());
                field.setForeign_key_column(schemaBean.getPk_column_name());
                field.setForeign_key_table(schemaBean.getPk_table_name());
            }
        }
    }

    static void pkResultsetToMap(ResultSet rs, Map<String, Integer> map) throws SQLException {
        if (rs != null) {
            while (rs.next()) {
                map.put(rs.getString(COLUMN_NAME), rs.getInt(KEY_SEQ));
            }
        }
    }

    static void fkResultsetToMap(ResultSet rs, Map<String, SchemaBean> map) throws SQLException {
        if (rs != null) {
            while (rs.next()) {
                map.put(rs.getString(FK_COLUMN_NAME), new SchemaBean(
                        rs.getString(PK_COLUMN_NAME),
                        rs.getString(PK_TABLE_NAME)
                ));
            }
        }
    }

    static void tbResultsetToSet(ResultSet rs, Set<String> set, Pattern p) throws SQLException {
        if (rs != null && set != null) {
            while (rs.next()) {
                String tableName = rs.getString(TABLE_NAME);

                if (is_mssql_cdc) {
                    String[] tableNames = tableName.split("_");
                    if (tableNames.length > 1) {
                        tableName = tableNames[1];
                    }
                }

                if (p == null || !p.matcher(tableName).matches()) {
                    set.add(tableName);
                }
            }
        }
    }

    static String getSchema(SchemaBean schemaBean) {
        String schema = "";
        if (schemaBean != null) {
            schema = schemaBean.getSchema();

            if (is_mssql_cdc) {
                String[] strs = schemaBean.getTablePattern().split("_");
                if (strs.length > 0) {
                    schema = strs[0];
                }
            }
        }

        return schema;
    }
}
