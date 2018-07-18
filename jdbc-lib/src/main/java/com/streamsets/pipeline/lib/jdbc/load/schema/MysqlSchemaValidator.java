package com.streamsets.pipeline.lib.jdbc.load.schema;

import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class MysqlSchemaValidator extends SchemaFactory implements ISchemaValidator {
    private static final String TABLE_METADATA_TABLE_NAME_CONSTANT = "TABLE_NAME";

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
                    String schema = schemaBeans.get(0).getSchema();

                    for (SchemaBean schemaBean : schemaBeans) {
                        Pattern p = StringUtils.isBlank(schemaBean.getTableExceludePattern()) ? null : Pattern.compile(schemaBean.getTableExceludePattern());
                        tableRs = JdbcUtil.getTableMetadata(conn, null, schema, schemaBean.getTablePattern(), false);

                        // get table names
                        while (tableRs.next()) {
                            String tableName = tableRs.getString(TABLE_METADATA_TABLE_NAME_CONSTANT);

                            if (p == null || !p.matcher(tableName).matches()) {
                                tableSet.add(tableName);
                            }
                        }
                    }

                    if (CollectionUtils.isNotEmpty(tableSet)) {
                        for (String tableName : tableSet) {
                            colRs = JdbcUtil.getColumnMetadata(conn, schema, tableName);
                            pkRs = JdbcUtil.getPrimaryKeysResultSet(conn, schema, tableName);
                            fkRs = JdbcUtil.getReferredTablesResultSet(conn, schema, tableName);
                            RelateDataBaseTable table = new RelateDataBaseTable(tableName);
                            List<RelateDatabaseField> fields = new ArrayList<>();

                            while (colRs.next()) {
                                RelateDatabaseField field = new RelateDatabaseField(
                                        colRs.getString(COLUMN_NAME),
                                        colRs.getString(TABLE_NAME),
                                        colRs.getString(TYPE_NAME)
                                );

                                setPrimaryKey(field, pkRs);
                                setForeignKey(field, fkRs);

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

    private static void setPrimaryKey(RelateDatabaseField field, ResultSet rs) throws SQLException {
        if (field != null && rs != null) {
            rs.beforeFirst();
            while (rs.next()) {
                if (field.getField_name().equals(rs.getString(COLUMN_NAME))) {
                    field.setKey(PRI);
                    field.setPrimary_key_position(rs.getShort(KEY_SEQ));
                    break;
                }
            }
        }
    }

    private static void setForeignKey(RelateDatabaseField field, ResultSet rs) throws SQLException {
        if (field != null && rs != null) {
            rs.beforeFirst();
            while (rs.next()) {
                if (field.getField_name().equals(rs.getString(FK_COLUMN_NAME))) {
                    field.setForeign_key_column(rs.getString(PK_COLUMN_NAME));
                    field.setForeign_key_table(rs.getString(PK_TABLE_NAME));
                    break;
                }
            }
        }
    }
}
