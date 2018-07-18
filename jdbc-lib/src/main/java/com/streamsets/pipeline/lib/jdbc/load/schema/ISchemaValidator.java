package com.streamsets.pipeline.lib.jdbc.load.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public interface ISchemaValidator {
    /**
     * load oracle schema
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    List<RelateDataBaseTable> validateSchema(Connection conn, Statement statement, List<?> tableCongifs) throws SQLException;

    List<RelateDataBaseTable> validateSchema(Connection conn, List<SchemaBean> schemaBeans) throws SQLException;
}
