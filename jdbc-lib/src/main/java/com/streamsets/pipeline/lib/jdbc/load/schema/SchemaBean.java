package com.streamsets.pipeline.lib.jdbc.load.schema;

public class SchemaBean {
    private String schema;

    private String tablePattern;

    private String tableExceludePattern;

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
