package com.streamsets.pipeline.lib.jdbc.oracle.schema;

import java.util.List;

public class RelateDataBaseTable {

    private String table_name;

    private List<RelateDatabaseField> fields;

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public List<RelateDatabaseField> getFields() {
        return fields;
    }

    public void setFields(List<RelateDatabaseField> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RelateDataBaseTable{");
        sb.append("table_name='").append(table_name).append('\'');
        sb.append(", fields=").append(fields);
        sb.append('}');
        return sb.toString();
    }
}
