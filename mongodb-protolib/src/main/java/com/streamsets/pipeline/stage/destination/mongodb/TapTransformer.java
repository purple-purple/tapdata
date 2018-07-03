/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.mongodb;

import com.mongodb.client.model.*;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.mongodb.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.mortbay.util.StringUtil;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TapTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(TapTransformer.class);

    Document mapConfig = new Document();

    public static final String CONFIG_PREFIX = "configBean.";
    public static final String MONGO_CONFIG_PREFIX = CONFIG_PREFIX + "mongoConfig.";

    private enum RecordTableEnum {
        ORALCE_CDC_TALBE("oracle.cdc.table"),
        JDBC_TABLES("jdbc.tables");

        String fieldName;

        RecordTableEnum(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    public TapTransformer(
            MongoTargetConfigBean config,
            List<Stage.ConfigIssue> issues,
            Stage.Context context
    ) {
        if (config.isCloningMode) {
            if (StringUtils.isBlank(config.schema))
//                config.schema = "[{\"table_name\":\"SUB\",\"fields\":[{\"field_name\":\"PK_ONE\",\"table_name\":\"SUB\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":10,\"scale\":0},{\"field_name\":\"VALUE\",\"table_name\":\"SUB\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"KEY\",\"table_name\":\"SUB\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_WAREHOUSE\",\"fields\":[{\"field_name\":\"COLUMN_11\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"W_TAX\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":4,\"scale\":4},{\"field_name\":\"COLUMN_10\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":30,\"scale\":0},{\"field_name\":\"W_YTD\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":12,\"scale\":2},{\"field_name\":\"W_NAME\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"COLUMN_12\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"W_STATE\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"W_ID\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"W_STREET_1\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"W_CITY\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"W_STREET_2\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"W_ZIP\",\"table_name\":\"BMSQL_WAREHOUSE\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_ITEM\",\"fields\":[{\"field_name\":\"I_NAME\",\"table_name\":\"BMSQL_ITEM\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"I_PRICE\",\"table_name\":\"BMSQL_ITEM\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":5,\"scale\":2},{\"field_name\":\"I_ID\",\"table_name\":\"BMSQL_ITEM\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"I_DATA\",\"table_name\":\"BMSQL_ITEM\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"I_IM_ID\",\"table_name\":\"BMSQL_ITEM\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"ARRAY\",\"fields\":[{\"field_name\":\"PK_ONE\",\"table_name\":\"ARRAY\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":10,\"scale\":0},{\"field_name\":\"VALUE\",\"table_name\":\"ARRAY\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"KEY\",\"table_name\":\"ARRAY\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"SUBM\",\"fields\":[{\"field_name\":\"AA\",\"table_name\":\"SUBM\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"SUB\",\"foreign_key_column\":\"PK_ONE\",\"key\":null,\"precision\":10,\"scale\":0},{\"field_name\":\"SS\",\"table_name\":\"SUBM\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"PK_TWO\",\"table_name\":\"SUBM\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":10,\"scale\":0}]},{\"table_name\":\"BMSQL_NEW_ORDER\",\"fields\":[{\"field_name\":\"NO_W_ID\",\"table_name\":\"BMSQL_NEW_ORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":\"BMSQL_OORDER\",\"foreign_key_column\":\"O_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"NO_D_ID\",\"table_name\":\"BMSQL_NEW_ORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":2,\"foreign_key_table\":\"BMSQL_OORDER\",\"foreign_key_column\":\"O_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"NO_O_ID\",\"table_name\":\"BMSQL_NEW_ORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":3,\"foreign_key_table\":\"BMSQL_OORDER\",\"foreign_key_column\":\"O_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_STOCK\",\"fields\":[{\"field_name\":\"S_DIST_05\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_06\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_I_ID\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"NUMBER\",\"primary_key_position\":2,\"foreign_key_table\":\"BMSQL_ITEM\",\"foreign_key_column\":\"I_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_03\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_YTD\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_04\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_09\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_07\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_08\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_W_ID\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":\"BMSQL_WAREHOUSE\",\"foreign_key_column\":\"W_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"S_DATA\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_QUANTITY\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_ORDER_CNT\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_REMOTE_CNT\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_01\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_02\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"S_DIST_10\",\"table_name\":\"BMSQL_STOCK\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_ORDER_LINE\",\"fields\":[{\"field_name\":\"OL_O_ID\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":3,\"foreign_key_table\":\"BMSQL_OORDER\",\"foreign_key_column\":\"O_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"OL_DIST_INFO\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"OL_W_ID\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":\"BMSQL_OORDER\",\"foreign_key_column\":\"O_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"OL_NUMBER\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":4,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"OL_SUPPLY_W_ID\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_STOCK\",\"foreign_key_column\":\"S_I_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"OL_DELIVERY_D\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"TIMESTAMP(6)\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":6},{\"field_name\":\"OL_QUANTITY\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"OL_I_ID\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_STOCK\",\"foreign_key_column\":\"S_I_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"OL_AMOUNT\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":6,\"scale\":2},{\"field_name\":\"OL_D_ID\",\"table_name\":\"BMSQL_ORDER_LINE\",\"data_type\":\"NUMBER\",\"primary_key_position\":2,\"foreign_key_table\":\"BMSQL_OORDER\",\"foreign_key_column\":\"O_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_DISTRICT\",\"fields\":[{\"field_name\":\"D_W_ID\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":\"BMSQL_WAREHOUSE\",\"foreign_key_column\":\"W_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"D_YTD\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":12,\"scale\":2},{\"field_name\":\"D_CITY\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"D_ID\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"NUMBER\",\"primary_key_position\":2,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"D_TAX\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":4,\"scale\":4},{\"field_name\":\"D_NEXT_O_ID\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"D_STATE\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"D_ZIP\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"D_NAME\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"D_STREET_1\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"D_STREET_2\",\"table_name\":\"BMSQL_DISTRICT\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_CONFIG\",\"fields\":[{\"field_name\":\"CFG_VALUE\",\"table_name\":\"BMSQL_CONFIG\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"CFG_NAME\",\"table_name\":\"BMSQL_CONFIG\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_CUSTOMER\",\"fields\":[{\"field_name\":\"C_LAST\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_CREDIT\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_YTD_PAYMENT\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":12,\"scale\":2},{\"field_name\":\"C_STATE\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_W_ID\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":\"BMSQL_DISTRICT\",\"foreign_key_column\":\"D_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"C_ID\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":3,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"C_CITY\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_DATA\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_BALANCE\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":12,\"scale\":2},{\"field_name\":\"C_FIRST\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_SINCE\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"TIMESTAMP(6)\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":6},{\"field_name\":\"C_DISCOUNT\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":4,\"scale\":4},{\"field_name\":\"C_CREDIT_LIM\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":12,\"scale\":2},{\"field_name\":\"C_STREET_1\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_PHONE\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_STREET_2\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_PAYMENT_CNT\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_DELIVERY_CNT\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_ZIP\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_MIDDLE\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"CHAR\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"C_D_ID\",\"table_name\":\"BMSQL_CUSTOMER\",\"data_type\":\"NUMBER\",\"primary_key_position\":2,\"foreign_key_table\":\"BMSQL_DISTRICT\",\"foreign_key_column\":\"D_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0}]},{\"table_name\":\"BMSQL_HISTORY\",\"fields\":[{\"field_name\":\"H_DATE\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"TIMESTAMP(6)\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":6},{\"field_name\":\"HIST_ID\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"H_DATA\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"H_C_ID\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_CUSTOMER\",\"foreign_key_column\":\"C_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"H_D_ID\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_DISTRICT\",\"foreign_key_column\":\"D_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"H_W_ID\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_DISTRICT\",\"foreign_key_column\":\"D_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"H_C_W_ID\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_CUSTOMER\",\"foreign_key_column\":\"C_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"H_C_D_ID\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_CUSTOMER\",\"foreign_key_column\":\"C_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"H_AMOUNT\",\"table_name\":\"BMSQL_HISTORY\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":6,\"scale\":2}]},{\"table_name\":\"BMSQL_OORDER\",\"fields\":[{\"field_name\":\"O_W_ID\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":\"BMSQL_CUSTOMER\",\"foreign_key_column\":\"C_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"O_OL_CNT\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"O_ALL_LOCAL\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"O_ENTRY_D\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"TIMESTAMP(6)\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":6},{\"field_name\":\"O_ID\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":3,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"O_C_ID\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"BMSQL_CUSTOMER\",\"foreign_key_column\":\"C_ID\",\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"O_D_ID\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":2,\"foreign_key_table\":\"BMSQL_CUSTOMER\",\"foreign_key_column\":\"C_ID\",\"key\":\"PRI\",\"precision\":0,\"scale\":0},{\"field_name\":\"O_CARRIER_ID\",\"table_name\":\"BMSQL_OORDER\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0}]},{\"table_name\":\"ARRAYD\",\"fields\":[{\"field_name\":\"AA\",\"table_name\":\"ARRAYD\",\"data_type\":\"NUMBER\",\"primary_key_position\":0,\"foreign_key_table\":\"ARRAY\",\"foreign_key_column\":\"PK_ONE\",\"key\":null,\"precision\":10,\"scale\":0},{\"field_name\":\"SS\",\"table_name\":\"ARRAYD\",\"data_type\":\"VARCHAR2\",\"primary_key_position\":0,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":null,\"precision\":0,\"scale\":0},{\"field_name\":\"PK_TWO\",\"table_name\":\"ARRAYD\",\"data_type\":\"NUMBER\",\"primary_key_position\":1,\"foreign_key_table\":null,\"foreign_key_column\":null,\"key\":\"PRI\",\"precision\":10,\"scale\":0}]}]";
            if (StringUtils.isNotBlank(config.schema)) {
                try {
                    generateClusterCloneMappings(config);
                } catch (IOException e) {
                    /*issues.add(context.createConfigIssue(
                            Groups.MAPPING.name(),
                            MONGO_CONFIG_PREFIX + "mapping",
                            Errors.MONGODB_36,
                            e.toString()
                    ));*/
                    LOG.error("Parse schema json string to list error: {}", e.toString());
                }
            } else {
                LOG.error("Schema is empty");
            }
        }

        if (StringUtils.isNotBlank(config.mapping)) {
            mapConfig = Document.parse(config.mapping);
            LOG.debug("Configured mapping value: " + mapConfig.get("mappings"));
        } else {
            issues.add(context.createConfigIssue(
                    Groups.MAPPING.name(),
                    MONGO_CONFIG_PREFIX + "mapping",
                    Errors.MONGODB_35
            ));
        }
    }

    public Map<String, List<WriteModel<Document>>> processRecord(Record record, Document doc) {
        //return new InsertOneModel<>(document);

        //String tableName = messageEntity.getTableName();
        //List<Mapping> mappings = tableMappings.get(tableName);
        //List<ProcessResult> processResults = new ArrayList<>();

        //for (Mapping mapping : mappings) {
        //LOG.debug("Processing record " + record);
        // System.out.println(record);
        // for(String attr: record.getHeader().getAttributeNames()){
        //     System.out.println(attr+" -- "+record.getHeader().getAttribute(attr));
        // }
        Map<String, List<WriteModel<Document>>> map = new HashMap<>();
//        List<WriteModel<Document>> models = new ArrayList<>();
        String operationCode = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
        String tableName = null;

        for (RecordTableEnum recordTableEnum : RecordTableEnum.values()) {
            if (recordTableEnum == RecordTableEnum.ORALCE_CDC_TALBE) {
                tableName = record.getHeader().getAttribute(recordTableEnum.getFieldName());
                if (StringUtils.isNotBlank(tableName)) {
                    break;
                }
            }
            if (recordTableEnum == RecordTableEnum.JDBC_TABLES) {
                tableName = record.getHeader().getAttribute(recordTableEnum.getFieldName());
                break;
            }
        }

        if (StringUtils.isBlank(tableName)) {
            LOG.warn("Not support this record {}", record.getHeader());
            return null;
        }

        String operation;
        if (operationCode != null) {
            operation = OperationType.getLabelFromStringCode(operationCode);
        } else {
            operation = record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE);
        }
        if (operation == null)
            operation = "";
        operation = operation.toUpperCase();

        String mappingsStr = mapConfig.getString("mappings");
        Object[] objs = (Object[]) JSON.parse(mappingsStr);

        for (Object obj : objs) {
            Map mapping = (Map) obj;
            String fromTable = (String) mapping.get("from_table");
            String toTable = (String) mapping.get("to_table");

            if (StringUtils.isNotBlank(fromTable) && fromTable.equals(tableName)) {
                WriteModel<Document> result = null;
                String relationship = (String) mapping.get("relationship");
                if (relationship == null)
                    relationship = "OneOne";
                switch (relationship) {
                    case "OplogClone":
                        result = applyOplog(record, doc);
                        break;
                    case "ManyOne":
                        result = embedMany(record, doc, operation, mapping);
                        break;

                    case "OneMany":
                        //logger.warn("Unsupport this relationship {}", relationship);
                        System.out.println("One Many not supported yet");
                        break;
                    case "OneOne":
                    default:
                        result = upsert(record, doc, operation, mapping);
                        break;
                }
                if (!map.containsKey(toTable)) {
                    map.put(toTable, new ArrayList<>());
                }
                if (result != null) {
                    map.get(toTable).add(result);
//                models.add(result);
                }
            }
        }
//        Iterator<Object> iterator = objs.iterator();
//        while (iterator.hasNext()) {
//            BSONObject mapping = (BSONObject) iterator.next();
//
//            WriteModel<Document> result = null;
//            String relationship = (String) mapping.get("relationship");
//            if(relationship == null)
//                relationship = "OneOne";
//            switch (relationship) {
//                case "OplogClone":
//                    result = applyOplog(record, doc);
//                    break;
//                case "ManyOne":
//                    result = embedMany(record, doc,operation, mapping);
//                    break;
//
//                case "OneMany":
//                    //logger.warn("Unsupport this relationship {}", relationship);
//                    System.out.println("One Many not supported yet");
//                    break;
//                case "OneOne":
//                default:
//                    result = upsert(record, doc, operation, mapping);
//                    break;
//            }
//            if (result != null) {
//                models.add(result);
//            }
//        }
//        for (BSONObject mapping : objs) {
//
//            WriteModel<Document> result = null;
//            String relationship = (String) mapping.get("relationship");
//            if(relationship == null)
//                relationship = "OneOne";
//            switch (relationship) {
//                case "OplogClone":
//                    result = applyOplog(record, doc);
//                    break;
//                case "ManyOne":
//                    result = embedMany(record, doc,operation, mapping);
//                    break;
//
//                case "OneMany":
//                    //logger.warn("Unsupport this relationship {}", relationship);
//                    System.out.println("One Many not supported yet");
//                    break;
//                case "OneOne":
//                default:
//                    result = upsert(record, doc, operation, mapping);
//                    break;
//            }
//            if (result != null) {
//                models.add(result);
//            }
//        }
        return map;

        /**
         new UpdateOneModel<>(
         new Document(
         removeLeadingSlash(mongoTargetConfigBean.uniqueKeyField),
         record.get(mongoTargetConfigBean.uniqueKeyField).getValueAsString()
         ),
         new Document("$set", document),
         new UpdateOptions().upsert(mongoTargetConfigBean.isUpsert)
         )
         */
    }

    private WriteModel<Document> applyOplog(Record record, Document doc) {
        Document o = (Document) doc.get("o");
        Document o2 = (Document) doc.get("o2");
        String op = doc.getString("op");
        UpdateOptions updateOptions = new UpdateOptions();

//        System.out.println("=====" +op+" o:"+ o +" ,,, o2:"+ o2 +"");
//        System.out.println(""+ doc);
//        System.out.println(o.get("_id"));
//        System.out.println("------\n\n");

        switch (op) {
            case "d":
                // Update delete = new Update().pull(targetPath, value);
                // processResult.setUpdate(delete);
                return new DeleteOneModel<>(o);  //@todo check existence of O
            case "i":
                // use upsert instead of insert
                if (o.get("_id") != null) {
                    updateOptions.upsert(true);
                    o2 = new Document("_id", o.get("_id"));
                } else {
                    // index creation op
                }
                // fall through
            case "u":
                if (o != null && o2 != null) {
                    if (o.get("_id") != null) {//@todo better way to determine ReplaceOne
                        return new ReplaceOneModel<>(o2, o, updateOptions);
                    }
                    return new UpdateOneModel(o2, o);
                } else {
                    LOG.warn("Missing o and o2 in update: {}", doc);
                }
                break;

            case "c":

            default:
                LOG.info("Unsupported oplog entry {}", doc);
        }
        return null;
    } // end meothd

    private WriteModel<Document> upsert(Record record, Document doc, String operation, Map mapping) {
        if (StringUtils.isBlank(operation)) {
            operation = "INSERT";
        }
        WriteModel<Document> writeModel = null;
        Document criteria = new Document();
        Document updateSpec = new Document();

        Object[] joinCondition = (Object[]) mapping.get("join_condition");

        for (Object obj : joinCondition) {
            Map condition = (Map) obj;
            String source = (String) condition.get("source");
            String target = (String) condition.get("target");
            if (source != null && target != null) {
                criteria.append(target, doc.get(source));
            }
        }
//        System.out.println("Upsert document"+ doc.toJson());
//        System.out.println("Upsert record"+ record);
//        System.out.println("operation:" +  operation);
//        System.out.println("================\n\n");
        LOG.debug("upsert criteria " + criteria);

        String targetPath = (String) mapping.get("target_path");

        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (targetPath != null && targetPath.length() > 0)
                key = targetPath + "." + key;
            updateSpec.append(key, val);
        }

        switch (operation) {
            case "DELETE":
                // Update delete = new Update().pull(targetPath, value);
                // processResult.setUpdate(delete);
                writeModel = new DeleteOneModel<>(criteria);
                break;
            case "UPDATE":
            case "INSERT":
//                updateSpec = new Document(targetPath, doc);
                writeModel = new UpdateOneModel(
                        criteria,
                        new Document("$set", updateSpec),
                        new UpdateOptions().upsert(true));
                break;
        }

        return writeModel;

    } // end meothd


    private WriteModel<Document> embedMany(Record record, Document doc, String operation, Map mapping) {
//        System.out.println(doc);
        if (StringUtils.isBlank(operation)) {
            operation = "INSERT";
        }
        String targetPath = (String) mapping.get("target_path");

        Document criteria = new Document();
        Document matchCriteria = new Document();
        Document updateSpec = new Document();

        Object[] joinCondition = (Object[]) mapping.get("join_condition");
        for (Object obj : joinCondition) {
            Map condition = (Map) obj;
            String source = (String) condition.get("source");
            String target = (String) condition.get("target");
            if (StringUtils.isNotBlank(source) && StringUtils.isNotBlank(target)) {
                criteria.append(target, doc.get(source));
            }
        }

        Object[] matchCondition = (Object[]) mapping.get("match_condition");
        for (Object obj : matchCondition) {
            Map condition = (Map) obj;
            String source = (String) condition.get("source");
            String target = (String) condition.get("target");
            if (StringUtils.isNotBlank(source) && StringUtils.isNotBlank(target) && StringUtils.isNotBlank(targetPath)) {
                // criteria.append(target, doc.get(source));
//                 StringBuilder sb = new StringBuilder(targetPath).append(".$.").append(target.split("\\.", 1)[1]);
                matchCriteria.append(target.split("\\.", 2)[1], doc.get(source));

            }
        }
//        String op = "i"; //@todo, from Record.getHeader
        switch (operation) {
            case "DELETE":
                // Update delete = new Update().pull(targetPath, value);
                // processResult.setUpdate(delete);
//                writeModel = new DeleteOneModel<>(doc);
                updateSpec = new Document("$pull", new Document(targetPath, matchCriteria));
                break;
            case "UPDATE":
//                 Update update = new Update();
                criteria.append(targetPath, new Document("$elemMatch", matchCriteria));
                Document set = new Document();
                for (Map.Entry<String, Object> entry : doc.entrySet()) {
                    String key = entry.getKey();
                    StringBuilder sb = new StringBuilder(targetPath).append(".$.").append(key);
                    set.append(sb.toString(), entry.getValue());
                }
                // processResult.setUpdate(update);
//                StringBuilder sb = new StringBuilder(targetPath).append(".$.").append(target.split("\\.", 1)[1]);
                updateSpec.append("$set", set);
                break;
            case "INSERT":
                updateSpec = new Document("$addToSet", new Document(targetPath, doc));
//                updateSpec = new Document(targetPath, doc);
                break;
        }
        return new UpdateOneModel(
                criteria, updateSpec, new UpdateOptions().upsert(true)
        );
    }

    public static void generateClusterCloneMappings(MongoTargetConfigBean config) throws IOException {
        Mappings mappingBean = new Mappings();
        List<Mapping> mappings = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        // JSON String to ArrayList
        JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, RelateDataBaseTable.class);
        List<RelateDataBaseTable> schemaTables = (List<RelateDataBaseTable>) mapper.readValue(config.schema, javaType);

        for (RelateDataBaseTable schemaTable : schemaTables) {
            String tableName = schemaTable.getTable_name();
            List<RelateDatabaseField> fields = schemaTable.getFields();
            List<String> pks = new ArrayList<>();
            List<Map<String, String>> joinConditions = new ArrayList<>(fields.size());
            for (RelateDatabaseField field : fields) {
                if (field.getPrimary_key_position() > 0) {
                    Map<String, String> condition = new HashMap<>(2);
                    String fieldName = field.getField_name();
                    pks.add(fieldName);
                    condition.put("source", fieldName);
                    condition.put("target", fieldName);
                    joinConditions.add(condition);
                }
            }
            if (CollectionUtils.isEmpty(joinConditions)) {
                LOG.warn("Table {} without pks {}, can't exec cluster clone.", tableName, fields);
                continue;
            }
            Mapping mapping = new Mapping(tableName, tableName, pks, joinConditions);
            mappings.add(mapping);
        }

        // override mapping
        if (CollectionUtils.isNotEmpty(mappings)) {
            mappingBean.setMappings(new ObjectMapper().writeValueAsString(mappings));
            config.mapping = new ObjectMapper().writeValueAsString(mappingBean);
        }
    }
}
/**
 * private ProcessResult embedMany(MessageEntity msg, Mapping mapping) {
 * <p>
 * <p>
 * ProcessResult processResult = new ProcessResult();
 * <p>
 * Map<String, Object> value = msg.getAfter();
 * if (value == null) {
 * value = msg.getBefore();
 * }
 * String targetPath = mapping.getTarget_path();
 * String toTable = mapping.getTo_table();
 * <p>
 * Query query = new Query();
 * List<Map<String, String>> joinCondition = mapping.getJoin_condition();
 * for (Map<String, String> condition : joinCondition) {
 * for (Map.Entry<String, String> entry : condition.entrySet()) {
 * query.addCriteria(new Criteria().and(entry.getKey()).is(value.get(entry.getValue())));
 * }
 * }
 * String op = msg.getOp();
 * Query existsQuery = new Query();
 * Criteria elemMatch = new Criteria();
 * List<Map<String, String>> matchCondition = mapping.getMatch_condition();
 * for (Map<String, String> condition : matchCondition) {
 * for (Map.Entry<String, String> entry : condition.entrySet()) {
 * String key = entry.getKey();
 * <p>
 * existsQuery.addCriteria(new Criteria().and(key).is(value.get(entry.getValue())));
 * <p>
 * if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op) ||
 * ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(op)) {
 * elemMatch.and(key.split("\\.")[1]).is(value.get(entry.getValue()));
 * }
 * <p>
 * }
 * }
 * if (ConnectorConstant.MESSAGE_OPERATION_DELETE.equals(op) ||
 * ConnectorConstant.MESSAGE_OPERATION_UPDATE.equals(op)) {
 * query.addCriteria(new Criteria(targetPath).elemMatch(elemMatch));
 * }
 * processResult.setOp(op);
 * processResult.setCollectionName(toTable);
 * processResult.setQuery(query);
 * <p>
 * processResult.setFromTable(msg.getTableName());
 * processResult.setOffset(msg.getOffset());
 * <p>
 * switch (op) {
 * case ConnectorConstant.MESSAGE_OPERATION_DELETE:
 * Update delete = new Update().pull(targetPath, value);
 * processResult.setUpdate(delete);
 * break;
 * case ConnectorConstant.MESSAGE_OPERATION_UPDATE:
 * Update update = new Update();
 * for (Map.Entry<String, Object> entry : value.entrySet()) {
 * String key = entry.getKey();
 * StringBuilder sb = new StringBuilder(targetPath).append(".$.").append(key);
 * update.set(sb.toString(), entry.getValue());
 * }
 * processResult.setUpdate(update);
 * break;
 * case ConnectorConstant.MESSAGE_OPERATION_INSERT:
 * //                Update insert = new Update();
 * String pk = query.toString();
 * if (!pushDocMap.containsKey(pk)) {
 * Map<String, Object> processMap = new HashMap<>();
 * pushDocMap.put(pk, processMap);
 * }
 * <p>
 * Map<String, Object> processMap = pushDocMap.get(pk);
 * <p>
 * if (!processMap.containsKey("processResult")) {
 * processMap.put("processResult", processResult);
 * }
 * <p>
 * ProcessResult cacheProcess = (ProcessResult) processMap.get("processResult");
 * cacheProcess.setOffset(msg.getOffset());
 * int processSize = cacheProcess.getProcessSize();
 * cacheProcess.setProcessSize(++processSize);
 * <p>
 * if (!processMap.containsKey("values")) {
 * List<Map<String, Object>> values = new ArrayList<>();
 * processMap.put("values", values);
 * }
 * <p>
 * if (!processMap.containsKey("targetPath")) {
 * processMap.put("targetPath", targetPath);
 * }
 * <p>
 * List<Map<String, Object>> values = (List<Map<String, Object>>) processMap.get("values");
 * values.add(value);
 * //                Update.PushOperatorBuilder push = insert.push(targetPath);
 * //                push.each(new ArrayList<>());
 * //                boolean upsert = msg.getUpsert();
 * //                if (upsert) {
 * //                   insert.addToSet(targetPath, value);
 * //                } else {
 * //                    insert.push(targetPath, value);
 * //                }
 * //                processResult.setUpdate(insert);
 * break;
 * }
 * <p>
 * }
 * <p>
 * private void createTargetMongoIndexes(Job job) {
 * <p>
 * List<Mapping> mappings = job.getMappings();
 * for (Mapping mapping : mappings) {
 * String toTable = mapping.getTo_table();
 * String relationship = mapping.getRelationship();
 * if (ConnectorConstant.RELATIONSHIP_ONE_ONE.equals(relationship)) {
 * List<Map<String, String>> joinCondition = mapping.getJoin_condition();
 * if (CollectionUtils.isNotEmpty(joinCondition)) {
 * createIndexes(toTable, joinCondition);
 * }
 * } else {
 * List<Map<String, String>> matchCondition = mapping.getMatch_condition();
 * if (CollectionUtils.isNotEmpty(matchCondition)) {
 * createIndexes(toTable, matchCondition);
 * }
 * }
 * <p>
 * }
 * }
 * <p>
 * private void createIndexes( String toTable, List<Map<String, String>> condition) {
 * ClientMongoOperator targetClientOperator = context.getTargetClientOperator();
 * List<String> fieldNames = new ArrayList<>();
 * for (Map<String, String> map : condition) {
 * for (Map.Entry<String, String> entry : map.entrySet()) {
 * fieldNames.add(entry.getKey());
 * }
 * }
 * if (CollectionUtils.isNotEmpty(fieldNames)) {
 * List<IndexModel> indexModels =new ArrayList<>();
 * IndexModel model = new IndexModel(Indexes.ascending(fieldNames));
 * model.getOptions().name(UUID.randomUUID().toString());
 * indexModels.add(model);
 * targetClientOperator.createIndexes(toTable, indexModels);
 * <p>
 * logger.info("Created target table {}'s indexes [{}]", toTable, fieldNames);
 * }
 * <p>
 * }
 * <p>
 * private Map<String, List<Mapping>> adaptMappingsToTableMappings(List<Mapping> mappings ) {
 * Map<String, List<Mapping>> tableMappings = new HashMap<>();
 * for (Mapping mapping : mappings) {
 * String fromTable = mapping.getFrom_table();
 * if (tableMappings.containsKey(fromTable)) {
 * tableMappings.get(fromTable).add(mapping);
 * } else {
 * List<Mapping> tableMap = new ArrayList<>();
 * tableMap.add(mapping);
 * tableMappings.put(fromTable, tableMap);
 * }
 * }
 * return tableMappings;
 * }
 * <p>
 * private List<Map<String, String>> reverseConditionMapKeyValue(List<Map<String, String>> conditions) {
 * List<Map<String, String>> processedConditions = new ArrayList<>();
 * if (CollectionUtils.isNotEmpty(conditions)) {
 * for (Map<String, String> condition : conditions) {
 * Map<String, String> processedCondition = new HashMap<>();
 * processedCondition.put(condition.get("target"), condition.get("source"));
 * processedConditions.add(processedCondition);
 * }
 * }
 * return processedConditions;
 * }
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * {
 * "from_table" : "BMSQL_ORDER_LINE",
 * "to_table" : "order",
 * "join_condition" : [
 * {
 * "source" : "OL_O_ID",
 * "target" : "O_ID"
 * },
 * {
 * "source" : "OL_W_ID",
 * "target" : "O_W_ID"
 * },
 * {
 * "source" : "OL_D_ID",
 * "target" : "O_D_ID"
 * }
 * ],
 * "relationship" : "ManyOne",
 * "target_path" : "BMSQL_ORDER_LINE",
 * "match_condition" : [
 * {
 * "source" : "OL_O_ID",
 * "target" : "BMSQL_ORDER_LINE.OL_O_ID"
 * },
 * {
 * "source" : "OL_W_ID",
 * "target" : "BMSQL_ORDER_LINE.OL_W_ID"
 * },
 * {
 * "source" : "OL_NUMBER",
 * "target" : "BMSQL_ORDER_LINE.OL_NUMBER"
 * },
 * {
 * "source" : "OL_D_ID",
 * "target" : "BMSQL_ORDER_LINE.OL_D_ID"
 * }
 * ]
 * }
 * <p>
 * <p>
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 * <p>
 * SEQ -- 1
 * oracle.cdc.SSN -- 0
 * operation type UPDATE
 * upsert criteria Document{{W_ID=null}}
 * update spec Document{{W_ID=null}}
 * Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 * schema -- TAPDATA
 * sdc.operation.type -- 3
 * jdbc.W_YTD.scale -- 2
 * oracle.cdc.operation -- UPDATE
 * oracle.cdc.scn -- 1240667
 * oracle.cdc.timestamp -- 2018-04-27 10:00:35
 * oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 * jdbc.W_YTD.precision -- 12
 * oracle.cdc.user -- TAPDATA
 * oracle.cdc.xid -- 10.19.4717
 * oracle.cdc.RS_ID --  0x000062.0000305a.0034
 * oracle.cdc.table -- BMSQL_WAREHOUSE
 * SEQ -- 1
 */


/**
 SEQ -- 1
 oracle.cdc.SSN -- 0
 operation type UPDATE
 upsert criteria Document{{W_ID=null}}
 update spec Document{{W_ID=null}}
 Record[headers='HeaderImpl[ 0x000062.0000305a.0034 ::0]' data='Field[MAP:{W_YTD=Field[DECIMAL:87050177.99]}]']
 schema -- TAPDATA
 sdc.operation.type -- 3
 jdbc.W_YTD.scale -- 2
 oracle.cdc.operation -- UPDATE
 oracle.cdc.scn -- 1240667
 oracle.cdc.timestamp -- 2018-04-27 10:00:35
 oracle.cdc.rowId -- AAAE6sAAFAAAACWAAB
 jdbc.W_YTD.precision -- 12
 oracle.cdc.user -- TAPDATA
 oracle.cdc.xid -- 10.19.4717
 oracle.cdc.RS_ID --  0x000062.0000305a.0034
 oracle.cdc.table -- BMSQL_WAREHOUSE
 SEQ -- 1

 */