package com.streamsets.pipeline.stage.common.mongodb;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseSchemaTableColumns {

    private String tableName;

    private String columnName;

    private String dataType;

    private Long dataLength;

    /**
     * oracle numbner type precision
     */
    private Integer precision;

    /**
     * oracle number type scale
     */
    private Integer scale;

    public DatabaseSchemaTableColumns() {
    }

    public DatabaseSchemaTableColumns(ResultSet resultSet) throws SQLException {

        this.tableName = resultSet.getString(1);
        this.columnName = resultSet.getString(2);
        this.dataType = resultSet.getString(3);
        this.dataLength = resultSet.getLong(4);
        this.precision = resultSet.getInt(5);
        this.scale = resultSet.getInt(6);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Long getDataLength() {
        return dataLength;
    }

    public void setDataLength(Long dataLength) {
        this.dataLength = dataLength;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OracleSchemaTableColumns{");
        sb.append("tableName='").append(tableName).append('\'');
        sb.append(", columnName='").append(columnName).append('\'');
        sb.append(", dataType='").append(dataType).append('\'');
        sb.append(", dataLength=").append(dataLength);
        sb.append(", precision=").append(precision);
        sb.append(", scale=").append(scale);
        sb.append('}');
        return sb.toString();
    }
}
