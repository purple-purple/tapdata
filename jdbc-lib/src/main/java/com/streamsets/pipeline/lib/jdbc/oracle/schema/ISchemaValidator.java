package com.streamsets.pipeline.lib.jdbc.oracle.schema;

import org.codehaus.jackson.JsonProcessingException;

import java.io.IOException;
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
    List<RelateDataBaseTable> validateSchema(Connection conn, String databaseOwner, Statement statement) throws SQLException;
}
