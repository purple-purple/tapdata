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
            if (StringUtils.isNotBlank(config.schema)) {
                try {
                    generateClusterCloneMappings(config);
                } catch (IOException e) {
                    issues.add(context.createConfigIssue(
                            Groups.MAPPING.name(),
                            MONGO_CONFIG_PREFIX + "mapping",
                            Errors.MONGODB_36,
                            e.toString()
                    ));
                }
            } else {
                issues.add(context.createConfigIssue(
                        Groups.MAPPING.name(),
                        MONGO_CONFIG_PREFIX + "schema",
                        Errors.MONGODB_37
                ));
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

        // get table name
        for (RecordTableEnum recordTableEnum : RecordTableEnum.values()) {
            tableName = record.getHeader().getAttribute(recordTableEnum.getFieldName());
            if (StringUtils.isNotBlank(tableName)) {
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
                if (StringUtils.isBlank(targetPath)) {
                    writeModel = new DeleteOneModel<>(criteria);
                } else {
                    writeModel = new UpdateManyModel(criteria, new Document("$unset", new Document(targetPath, 1)));
                }
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
//        JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, RelateDataBaseTable.class);
//        List<RelateDataBaseTable> schemaTables = (List<RelateDataBaseTable>) mapper.readValue(config.schema, javaType);
        RelateDataBases relateDataBases = mapper.readValue(config.schema, RelateDataBases.class);
        List<RelateDataBaseTable> schemaTables = relateDataBases.getTables();

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