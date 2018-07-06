package com.streamsets.pipeline.stage.common.mongodb;

import java.util.List;

public class RelateDataBases {

    private String database;

    private List<RelateDataBaseTable> tables;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public List<RelateDataBaseTable> getTables() {
        return tables;
    }

    public void setTables(List<RelateDataBaseTable> tables) {
        this.tables = tables;
    }
}
