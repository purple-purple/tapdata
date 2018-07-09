package com.streamsets.pipeline.lib.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public interface JdbcLoadSchema {
    /**
     * @param connection
     * @param statement
     * @return
     * @throws IOException
     * @throws SQLException
     */
    String getTableSchemasJson(Connection connection, Statement statement) throws IOException, SQLException;

    String getTableSchemasJson(Connection connection, Statement statement, List<?> tableConfigs) throws IOException, SQLException;
}
