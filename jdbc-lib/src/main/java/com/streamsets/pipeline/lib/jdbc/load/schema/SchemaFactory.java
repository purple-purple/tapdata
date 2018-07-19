package com.streamsets.pipeline.lib.jdbc.load.schema;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean;
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

public class SchemaFactory {

    private final static Logger LOG = LoggerFactory.getLogger(SchemaFactory.class);

    private final static String TABLECONFIGBEAN = "com.streamsets.pipeline.stage.origin.jdbc.table.TableConfigBean";
    private final static String SCHEMATABLECONFIGBEAN = "com.streamsets.pipeline.stage.origin.jdbc.cdc.SchemaTableConfigBean";

    final static String TABLE_NAME = "TABLE_NAME";
    final static String COLUMN_NAME = "COLUMN_NAME";
    final static String TYPE_NAME = "TYPE_NAME";
    final static String PRI = "PRI";
    final static String KEY_SEQ = "KEY_SEQ";
    final static String PK_TABLE_NAME = "PKTABLE_NAME";
    final static String PK_COLUMN_NAME = "PKCOLUMN_NAME";
    final static String FK_COLUMN_NAME = "FKCOLUMN_NAME";

    private final static String LOAD_SCHEMA_ERROR = "Load {} schema error: {}";


    public static List<SchemaBean> getSchemaConfigs(List<?> tableConfigs) {
        List<SchemaBean> schemaConfigBeans = new ArrayList<>();

        if (null != tableConfigs && CollectionUtils.isNotEmpty(tableConfigs)) {
            Object firstField = tableConfigs.get(0);

            if (firstField != null) {
                String typeName = firstField.getClass().getTypeName();

                if (StringUtils.isNotBlank(typeName)) {
                    if (typeName.equals(TABLECONFIGBEAN)) {
                        // jdbc cdc
                        for (Object field : tableConfigs) {
                            SchemaBean schemaBean = new SchemaBean();

                            schemaBean.setSchema(((TableConfigBean) field).schema);
                            schemaBean.setTablePattern(((TableConfigBean) field).tablePattern);
                            schemaBean.setTableExceludePattern(((TableConfigBean) field).tableExclusionPattern);

                            schemaConfigBeans.add(schemaBean);
                        }
                    } else if (typeName.equals(SCHEMATABLECONFIGBEAN)) {
                        // jdbc table
                        for (Object field : tableConfigs) {
                            SchemaBean schemaBean = new SchemaBean();

                            schemaBean.setSchema(((SchemaTableConfigBean) field).schema);
                            schemaBean.setTablePattern(((SchemaTableConfigBean) field).table);
                            schemaBean.setTableExceludePattern(((SchemaTableConfigBean) field).excludePattern);

                            schemaConfigBeans.add(schemaBean);
                        }
                    }
                }
            }
        }

        return schemaConfigBeans;
    }

    public List<RelateDataBaseTable> loadSchemaList(HikariPoolConfigBean hikariConfigBean, List<?> tableConfigs) {
        List<RelateDataBaseTable> relateDataBaseTables = new ArrayList<>();
        List<SchemaBean> schemaBeans = getSchemaConfigs(tableConfigs);
        String connectionString = hikariConfigBean.connectionString;
        ISchemaValidator iSchemaValidator;
        HikariDataSource dataSource = null;
        Connection conn = null;
        Statement statement = null;

        if (CollectionUtils.isEmpty(schemaBeans)) {
            throw new RuntimeException("Table config cannot be empty.");
        }

        try {
            try {
                dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
                conn = dataSource.getConnection();
            } catch (StageException e) {
                LOG.error(JdbcErrors.JDBC_00.getMessage(), e.toString(), e);
                return null;
            } catch (SQLException e) {
                LOG.error(JdbcErrors.JDBC_06.getMessage(), e.toString(), e);
                return null;
            }

            if (dataSource != null && conn != null) {
                if (connectionString.startsWith(DatabaseType.ORACLE.getType())) {
                    iSchemaValidator = new OracleSchemaValidator();
                    try {
                        statement = conn.createStatement();
                    } catch (SQLException e) {
                        LOG.error("Cannot create statement when load oracle schema: {}", e.toString(), e);
                        return null;
                    }

                    try {
//                        relateDataBaseTables = iSchemaValidator.validateSchema(conn, statement, tableConfigs);
                        relateDataBaseTables = iSchemaValidator.validateSchema(conn, schemaBeans);
                    } catch (SQLException e) {
                        LOG.error(LOAD_SCHEMA_ERROR, "oracle", e.toString(), e);
                        return null;
                    }
                } else if (connectionString.startsWith(DatabaseType.MYSQL.getType())) {
                    iSchemaValidator = new MysqlSchemaValidator();

                    try {
                        relateDataBaseTables = iSchemaValidator.validateSchema(conn, schemaBeans);
                    } catch (SQLException e) {
                        LOG.error(LOAD_SCHEMA_ERROR, "mysql", e.toString(), e);
                        return null;
                    }
                } else if (connectionString.startsWith(DatabaseType.SQLSERVER.getType())) {
                    iSchemaValidator = new SqlserverSchemaValidator();

                    try {
                        relateDataBaseTables = iSchemaValidator.validateSchema(conn, schemaBeans);
                    } catch (SQLException e) {
                        LOG.error(LOAD_SCHEMA_ERROR, "sqlserver", e.toString(), e);
                        return null;
                    }
                }
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
                LOG.error("Parse schema list to json failed: {}", e.toString(), e);
                return null;
            }
        } else {
            LOG.error("Load schema list error");
            return null;
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
}
