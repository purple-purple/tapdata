package com.streamsets.pipeline.lib.jdbc.load.schema;

public enum DatabaseType {
    ORACLE("jdbc:oracle"),
    MYSQL("jdbc:mysql"),
    SQLSERVER("jdbc:sqlserver");

    private final String type;

    DatabaseType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
