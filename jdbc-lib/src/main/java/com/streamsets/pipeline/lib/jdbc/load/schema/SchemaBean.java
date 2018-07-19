package com.streamsets.pipeline.lib.jdbc.load.schema;

public class SchemaBean {
    private String schema;

    private String tablePattern;

    private String tableExceludePattern;

    private String pk_column_name;

    private String pk_table_name;

    public SchemaBean() {
    }

    public SchemaBean(String pk_column_name, String pk_table_name) {
        this.pk_column_name = pk_column_name;
        this.pk_table_name = pk_table_name;
    }

    public String getPk_column_name() {
        return pk_column_name;
    }

    public void setPk_column_name(String pk_column_name) {
        this.pk_column_name = pk_column_name;
    }

    public String getPk_table_name() {
        return pk_table_name;
    }

    public void setPk_table_name(String pk_table_name) {
        this.pk_table_name = pk_table_name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTablePattern() {
        return tablePattern;
    }

    public void setTablePattern(String tablePattern) {
        this.tablePattern = tablePattern;
    }

    public String getTableExceludePattern() {
        return tableExceludePattern;
    }

    public void setTableExceludePattern(String tableExceludePattern) {
        this.tableExceludePattern = tableExceludePattern;
    }
}
