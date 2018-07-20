package com.streamsets.pipeline.lib.jdbc.load.schema;

import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Pattern;

public class MysqlSchemaValidator extends SchemaFactory implements ISchemaValidator {

    @Override
    public List<RelateDataBaseTable> validateSchema(Connection conn, Statement statement, List<?> tableCongifs) throws SQLException {
        return null;
    }

    @Override
    public List<RelateDataBaseTable> validateSchema(Connection conn, List<SchemaBean> schemaBeans) throws SQLException {
        List<RelateDataBaseTable> relateDataBaseTables = new ArrayList<>();
        ResultSet tableRs = null;
        ResultSet colRs = null;
        ResultSet pkRs = null;
        ResultSet fkRs = null;

        try {
            if (conn != null) {
                if (schemaBeans != null && CollectionUtils.isNotEmpty(schemaBeans)) {
                    Set<String> tableSet = new HashSet<>();
                    String schema = "";

                    for (SchemaBean schemaBean : schemaBeans) {
                        if (StringUtils.isNotBlank(schemaBean.getSchema()) && StringUtils.isNotBlank(schemaBean.getTablePattern())) {
                            Pattern p = StringUtils.isBlank(schemaBean.getTableExceludePattern()) ? null : Pattern.compile(schemaBean.getTableExceludePattern());
                            tableRs = JdbcUtil.getTableMetadata(conn, null, schemaBean.getSchema(), schemaBean.getTablePattern(), false);

                            // get schema
                            schema = getSchema(schemaBean);

                            // trans table resultset to set(unique)
                            tbResultsetToSet(tableRs, tableSet, p);
                        } else {
                            return null;
                        }
                    }

                    if (CollectionUtils.isNotEmpty(tableSet)) {
                        for (String tableName : tableSet) {
                            RelateDataBaseTable table = new RelateDataBaseTable(tableName);
                            List<RelateDatabaseField> fields = new ArrayList<>();

                            colRs = JdbcUtil.getColumnMetadata(conn, schema, tableName);
                            pkRs = JdbcUtil.getPrimaryKeysResultSet(conn, schema, tableName);
                            fkRs = JdbcUtil.getReferredTablesResultSet(conn, schema, tableName);
                            Map<String, Integer> pkMap = new HashMap<>();
                            Map<String, SchemaBean> fkMap = new HashMap<>();
                            pkResultsetToMap(pkRs, pkMap);
                            fkResultsetToMap(fkRs, fkMap);

                            while (colRs.next()) {
                                RelateDatabaseField field = new RelateDatabaseField(
                                        colRs.getString(COLUMN_NAME),
                                        colRs.getString(TABLE_NAME),
                                        colRs.getString(TYPE_NAME)
                                );

                                setPrimaryKey(field, pkMap);
                                setForeignKey(field, fkMap);

                                fields.add(field);
                            }

                            table.setFields(fields);

                            relateDataBaseTables.add(table);
                        }
                    }
                }
            }

        } finally {
            JdbcUtil.closeQuietly(tableRs);
            JdbcUtil.closeQuietly(colRs);
            JdbcUtil.closeQuietly(pkRs);
            JdbcUtil.closeQuietly(fkRs);
        }

        return relateDataBaseTables;
    }
}
