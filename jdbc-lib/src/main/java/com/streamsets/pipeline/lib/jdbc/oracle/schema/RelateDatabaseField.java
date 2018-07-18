package com.streamsets.pipeline.lib.jdbc.oracle.schema;

public class RelateDatabaseField {
    private String field_name;
    private String table_name;
    private String data_type = "";
    private int primary_key_position = 0;
    private String foreign_key_table;
    private String foreign_key_column;
    private String key;

    /**
     * Database numbner type precision
     */
    private Integer precision;

    /**
     * Database number type scale
     */
    private Integer scale;

    public RelateDatabaseField() {
    }

    public RelateDatabaseField(String field_name, String table_name, String data_type, int primary_key_position, String key) {
        this.field_name = field_name;
        this.table_name = table_name;
        this.data_type = data_type;
        this.primary_key_position = primary_key_position;
        this.key = key;
    }

    public RelateDatabaseField(DatabaseSchemaTableColumns column) {
        this.field_name = column.getColumnName();
        this.table_name = column.getTableName();
        this.data_type = column.getDataType();
        this.precision = column.getPrecision();
        this.scale = column.getScale();
    }

    public RelateDatabaseField(String field_name, String table_name, String data_type) {
        this.field_name = field_name;
        this.table_name = table_name;
        this.data_type = data_type;
    }

    public String getField_name() {
        return field_name;
    }

    public void setField_name(String field_name) {
        this.field_name = field_name;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getData_type() {
        return data_type;
    }

    public void setData_type(String data_type) {
        this.data_type = data_type;
    }

    public int getPrimary_key_position() {
        return primary_key_position;
    }

    public void setPrimary_key_position(int primary_key_position) {
        this.primary_key_position = primary_key_position;
    }

    public String getForeign_key_table() {
        return foreign_key_table;
    }

    public void setForeign_key_table(String foreign_key_table) {
        this.foreign_key_table = foreign_key_table;
    }

    public String getForeign_key_column() {
        return foreign_key_column;
    }

    public void setForeign_key_column(String foreign_key_column) {
        this.foreign_key_column = foreign_key_column;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
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
        final StringBuilder sb = new StringBuilder("RelateDatabaseField{");
        sb.append("field_name='").append(field_name).append('\'');
        sb.append(", table_name='").append(table_name).append('\'');
        sb.append(", data_type='").append(data_type).append('\'');
        sb.append(", primary_key_position=").append(primary_key_position);
        sb.append(", foreign_key_table='").append(foreign_key_table).append('\'');
        sb.append(", foreign_key_column='").append(foreign_key_column).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append(", precision=").append(precision);
        sb.append(", scale=").append(scale);
        sb.append('}');
        return sb.toString();
    }
}
