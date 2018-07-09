package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.lib.jdbc.oracle.schema.ISchemaValidator;
import com.streamsets.pipeline.lib.jdbc.oracle.schema.OracleSchemaValidator;
import com.streamsets.pipeline.lib.jdbc.oracle.schema.RelateDataBaseTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcOracleLoadSchemaImpl implements JdbcLoadSchema {

    private String databaseOwner;

    public JdbcOracleLoadSchemaImpl() {
        this.databaseOwner = StringUtils.isEmpty(databaseOwner) ? new String("") : this.databaseOwner;
    }

    /**
     * @param databaseOwner Owner of oracle database,if empty then load all schema
     */
    public JdbcOracleLoadSchemaImpl(String databaseOwner) {
        this.databaseOwner = StringUtils.isEmpty(databaseOwner) ? new String("") : databaseOwner.trim();
    }

    @Override
    public String getTableSchemasJson(Connection connection, Statement statement) throws IOException, SQLException {
        return getTableSchemasJson(connection, statement, null);
    }

    @Override
    public String getTableSchemasJson(Connection connection, Statement statement, List<?> tableConfigs) throws IOException, SQLException {
        ISchemaValidator oracleSchemaValidator = new OracleSchemaValidator();
        ObjectMapper mapper = new ObjectMapper();
        List<RelateDataBaseTable> tableSchemas = null;
        String tableSchemasJson = new String();

        if (null != connection && null != statement) {
            tableSchemas = oracleSchemaValidator.validateSchema(connection, databaseOwner, statement, tableConfigs);

            if (CollectionUtils.isNotEmpty(tableSchemas)) {
                // list to json
                tableSchemasJson = mapper.writeValueAsString(tableSchemas);
            }
        }

        return tableSchemasJson;
    }
}
