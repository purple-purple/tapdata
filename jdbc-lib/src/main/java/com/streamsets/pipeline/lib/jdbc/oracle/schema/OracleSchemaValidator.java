package com.streamsets.pipeline.lib.jdbc.oracle.schema;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleSchemaValidator implements ISchemaValidator {
    /**
     * 指定用户下的所有表及字段
     */
    public static final String LOAD_SCHEMA_TABLES_ALL_COLUMNS = "SELECT\n" +
            "  ut.table_name AS tableName,\n" +
            "  utc.column_name  columnName,\n" +
            "  utc.data_type    dataType,\n" +
            "  utc.DATA_LENGTH  dataLength,\n" +
            "  utc.DATA_PRECISION  precision,\n" +
            "  utc.DATA_SCALE  scale\n" +
            "FROM all_tables ut\n" +
            "  JOIN all_tab_columns utc ON ut.table_name = utc.table_name\n" +
            " AND ut.owner='%s'";
    /**
     * 指定所有用户下的表的所有主键
     */
    public static final String LOAD_SCHEMA_TABLES_ALL_PRIMARY_KEY = "SELECT\n" +
            "  ut.table_name AS tableName,\n" +
            "  ucc.COLUMN_NAME  columnName,\n" +
            "  ucc.constraint_name,\n" +
            "  ucc.position position\n" +
            "FROM all_tables ut\n" +
            "  JOIN all_cons_columns ucc ON ut.table_name = ucc.table_name\n" +
            "  LEFT JOIN all_CONSTRAINTS uc ON uc.constraint_name = ucc.constraint_name\n" +
            "WHERE uc.constraint_type = 'P' AND ut.owner='%s'";

    /**
     * 指定用户下所有表的外键
     */
    public static final String LOAD_SCHEMA_TABLES_ALL_FOREIGN_KEY = "SELECT\n" +
            "  ut.table_name AS tableName,\n" +
            "  ucc.COLUMN_NAME  columnName,\n" +
            "  ucc.constraint_name constraintName,\n" +
            "  ucc.position position,\n" +
            "  ucc2.table_name fkTableName,\n" +
            "  ucc2.COLUMN_NAME fkColumnName\n" +
            "FROM all_tables ut\n" +
            "  JOIN all_cons_columns ucc ON ut.table_name = ucc.table_name\n" +
            "  LEFT JOIN all_CONSTRAINTS uc ON uc.constraint_name = ucc.constraint_name\n" +
            "  LEFT JOIN all_cons_columns ucc2 ON uc.R_CONSTRAINT_NAME = ucc2.constraint_name\n" +
            "WHERE uc.constraint_type = 'R' AND ut.owner='%s'";


    @Override
    public List<RelateDataBaseTable> validateSchema(Connection conn,String databaseOwner, Statement statement) throws SQLException {
        List<RelateDataBaseTable> tables = null;
        ResultSet resultSet = null;
        try {

            // tables all columns
            Map<String, Map<String, DatabaseSchemaTableColumns>> tableColumnsMap = new HashMap<>();
            String tablesColumnsSql = String.format(LOAD_SCHEMA_TABLES_ALL_COLUMNS, databaseOwner);
            resultSet = statement.executeQuery(tablesColumnsSql);
            while (resultSet.next()) {
                DatabaseSchemaTableColumns tablesColumns = new DatabaseSchemaTableColumns(resultSet);
                adaptTableColumnsMap(tableColumnsMap, tablesColumns);
            }

            // tables primary key
            Map<String, Map<String, DatabaseSchemaConstraints>> pkTableMap = new HashMap<>();
            String tablesPrimaryKeySql = String.format(LOAD_SCHEMA_TABLES_ALL_PRIMARY_KEY, databaseOwner);
            resultSet = statement.executeQuery(tablesPrimaryKeySql);
            while (resultSet.next()) {
                DatabaseSchemaConstraints constraints = DatabaseSchemaConstraints.pkConstraints(resultSet);
                adaptConstraintsMap(pkTableMap, constraints);
            }

            // tables foreign key
            Map<String, Map<String, DatabaseSchemaConstraints>> fkTableMap = new HashMap<>();
            String tablesForeignKeySql = String.format(LOAD_SCHEMA_TABLES_ALL_FOREIGN_KEY, databaseOwner);
            resultSet = statement.executeQuery(tablesForeignKeySql);
            while (resultSet.next()) {
                DatabaseSchemaConstraints constraints = DatabaseSchemaConstraints.fkConstraints(resultSet);
                adaptConstraintsMap(fkTableMap, constraints);
            }
            tables = adaptToSchema(tableColumnsMap, pkTableMap, fkTableMap);
        } catch (SQLException e) {
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            /*if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }*/
        }

        return tables;
    }

    /**
     * adapt constraints map
     *
     * @param tableConsMap
     * @param constraints
     */
    public void adaptConstraintsMap(Map<String, Map<String, DatabaseSchemaConstraints>> tableConsMap, DatabaseSchemaConstraints constraints) {
        String tableName = constraints.getTableName();
        if (!tableConsMap.containsKey(tableName)) {
            tableConsMap.put(tableName, new HashMap<>());
        }
        tableConsMap.get(tableName).put(constraints.getColumnName(), constraints);
    }

    /**
     * adapt table columns map
     *
     * @param tableColumnsMap
     * @param tablesColumns
     */
    public void adaptTableColumnsMap(Map<String, Map<String, DatabaseSchemaTableColumns>> tableColumnsMap, DatabaseSchemaTableColumns tablesColumns) {
        String tableName = tablesColumns.getTableName();
        if (!tableColumnsMap.containsKey(tableName)) {
            tableColumnsMap.put(tableName, new HashMap<>());
        }
        tableColumnsMap.get(tableName).put(tablesColumns.getColumnName(), tablesColumns);
    }

    /**
     * adapt Database columns, constaints to connection schema
     *
     * @param tableColumnsMap
     * @param pkTableMap
     * @param fkTableMap
     * @return
     */
    public List<RelateDataBaseTable> adaptToSchema(Map<String, Map<String, DatabaseSchemaTableColumns>> tableColumnsMap, Map<String, Map<String, DatabaseSchemaConstraints>> pkTableMap, Map<String, Map<String, DatabaseSchemaConstraints>> fkTableMap) {
        List<RelateDataBaseTable> tables = new ArrayList<>();

        // adapte schema
        for (Map.Entry<String, Map<String, DatabaseSchemaTableColumns>> tableEntry : tableColumnsMap.entrySet()) {
            String tableName = tableEntry.getKey();
            Map<String, DatabaseSchemaTableColumns> value = tableEntry.getValue();

            // set result tables values
            List<RelateDatabaseField> fields = new ArrayList<>(value.size());
            RelateDataBaseTable table = new RelateDataBaseTable();
            table.setFields(fields);
            table.setTable_name(tableName);
            tables.add(table);

            // for loop table's columns
            for (Map.Entry<String, DatabaseSchemaTableColumns> columnsEntry : value.entrySet()) {
                String columnName = columnsEntry.getKey();
                DatabaseSchemaTableColumns column = columnsEntry.getValue();
                RelateDatabaseField field = new RelateDatabaseField(column);
                fields.add(field);

                // set pk info
                if (pkTableMap.containsKey(tableName)) {
                    Map<String, DatabaseSchemaConstraints> pkConstraintMap = pkTableMap.get(tableName);
                    if (pkConstraintMap.containsKey(columnName)) {
                        DatabaseSchemaConstraints pkConstraints = pkConstraintMap.get(columnName);
                        field.setPrimary_key_position(pkConstraints.getPosition());
                        field.setKey(ConnectorConstant.SCHEMA_PRIMARY_KEY);
                    }
                }

                // set fk info
                if (fkTableMap.containsKey(tableName)) {
                    Map<String, DatabaseSchemaConstraints> fkConstraintMap = fkTableMap.get(tableName);
                    if (fkConstraintMap.containsKey(columnName)) {
                        DatabaseSchemaConstraints fkConstraints = fkConstraintMap.get(columnName);
                        field.setForeign_key_column(fkConstraints.getFkColumnName());
                        field.setForeign_key_table(fkConstraints.getFkTableName());
                    }
                }
            }
        }

        return tables;
    }
}
