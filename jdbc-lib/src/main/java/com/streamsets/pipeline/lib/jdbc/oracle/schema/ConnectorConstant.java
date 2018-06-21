package com.streamsets.pipeline.lib.jdbc.oracle.schema;

import java.util.HashMap;
import java.util.Map;

public class ConnectorConstant {

    /**
     * DML操作的event
     */
    public static final long DML_EVENT = 2;

    public static final String SCHEDULED = "scheduled";

    public static final String RUNNING = "running";

    public static final String PAUSED = "paused";

    public static final String ERROR = "error";

    public static final String JOB_COLLECTION = "Jobs";

    public static final String INCONSISTENT_DATA_COLLECTION = "InconsistentData";

    public static final String JOB_VIEW_COLLECTION = "JobConnections";

    public static final String CONNECTION_COLLECTION = "Connections";

    public static final String WORKER_COLLECTION = "Workers";

    public static final String SETTING_COLLECTION = "Settings";

    public static final String MESSAGE_COLLECTION = "message";

    public static final String JOB_STATUS_FIELD = "status";


    /**
     * sync point
     */

    public static final String SYNC_POINT_FIELD = "sync_point";

    public static final String SYNC_TIME_FIELD = "sync_time";

    public static final String SYNC_POINT_CURRENT = "current";

    public static final String SYNC_POINT_SYNC_TIME = "sync_time";

    public static final String SYNC_POINT_BEGINNING = "beginning";

    /**
     * =================== relationship =====================
     */

    public static final String RELATIONSHIP_ONE_ONE = "OneOne";
    public static final String RELATIONSHIP_MANY_ONE = "ManyOne";
    public static final String RELATIONSHIP_ONE_MANY = "OneMany";

    /**
     * =================== mapping_template =====================
     */
    public static final String MAPPING_TEMPLATE_CUSTOM = "custom";

    public static final String MAPPING_TEMPLATE_CLUSTER_CLONE = "cluster-clone";


    /**
     * =================== worker type =====================
     */

    public static final String WORKER_TYPE_CONNECTOR = "connector";
    public static final String WORKER_TYPE_TRANSFORMER = "transformer";


    /**
     * =================== primary key =====================
     */
    public static final String SCHEMA_PRIMARY_KEY = "PRI";

    public static final String APP_TRANSFORMER = "transformer";


    public static final String MESSAGE_OPERATION_INSERT = "i";
    public static final String MESSAGE_OPERATION_DELETE = "d";
    public static final String MESSAGE_OPERATION_UPDATE = "u";

    /**
     * =================== sync way =====================
     */
    public static final String SYNC_TYPE_INITIAL_SYNC = "initial_sync";
    public static final String SYNC_TYPE_CDC = "cdc";
    public static final String SYNC_TYPE_ALL = "all";

    public enum DatabaseType {
        MYSQL("mysql"),
        ORACLE("oracle"),
        MONGODB("mongodb"),
        MSSQL("mssql");

        private String type;

        DatabaseType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        private static final Map<String, DatabaseType> map = new HashMap<>();

        static {
            for (DatabaseType databaseType : DatabaseType.values()) {
                map.put(databaseType.getType(), databaseType);
            }
        }

        public static DatabaseType fromString(String databaseType) {
            return map.get(databaseType);
        }
    }
}
