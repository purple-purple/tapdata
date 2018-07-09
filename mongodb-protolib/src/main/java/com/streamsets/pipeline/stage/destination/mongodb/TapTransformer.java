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
import com.streamsets.pipeline.lib.operation.OperationType;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TapTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(TapTransformer.class);

    Document mapConfig = new Document();

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
    
    public TapTransformer(MongoTargetConfigBean config){
        System.out.println("TapMongo initializing v3.2.0.18");
        if(config.mapping!=null && config.mapping.length()>0){
            mapConfig = Document.parse(config.mapping);
            LOG.debug("configured mapping value: "+ mapConfig.get("mappings"));
            //System.out.println();
            //Document d = (Document)mapConfig.get("mappings");
            // System.out.println("configured mapping value: "+ mapConfig.get("join_condition"));
            // System.out.println("whole map");
            // System.out.println(mapConfig);
        }
        else{
            // cluster clone mode
            LOG.debug("Mapping is empty and cloning mode is "+ config.isCloningMode);
            if(config.isCloningMode){
                Document doc = new Document("source", "_id");
                doc.append("target", "_id");
                List l = new ArrayList<Document>(1);
                l.add(doc);
                mapConfig.put("join_condition", l);
                mapConfig.put("relationship", "OplogClone");
            }
        }

    }
    public Map<String, List<WriteModel<Document>>> processRecord(Record record, Document doc){
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

        for (RecordTableEnum  recordTableEnum: RecordTableEnum.values()) {
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
        if(operation == null )
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
                if(relationship == null)
                    relationship = "OneOne";
                switch (relationship) {
                    case "OplogClone":
                        result = applyOplog(record, doc);
                        break;
                    case "ManyOne":
                        result = embedMany(record, doc,operation, mapping);
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

    private WriteModel<Document> applyOplog( Record record, Document doc) {
        Document o = (Document)doc.get("o");
        Document o2 = (Document)doc.get("o2");
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
                if(o.get("_id")!=null){
                    updateOptions.upsert(true);
                    o2 = new Document("_id", o.get("_id"));
                }
                else{
                    // index creation op
                }
                // fall through
            case "u":
                if(o!=null && o2!=null){
                    if(o.get("_id")!=null){//@todo better way to determine ReplaceOne
                        return new ReplaceOneModel<>(o2, o, updateOptions);
                    }
                    return new UpdateOneModel( o2, o);
                }
                else{
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
            if(source!=null && target!=null){
                criteria.append(target, doc.get(source));
            }
        }
//        System.out.println("Upsert document"+ doc.toJson());
//        System.out.println("Upsert record"+ record);
//        System.out.println("operation:" +  operation);
//        System.out.println("================\n\n");
        LOG.debug("upsert criteria " +  criteria);

        String targetPath = (String) mapping.get("target_path");

        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if(targetPath!=null && targetPath.length()>0)
                key = targetPath +"."+key;
            updateSpec.append(key, val);
        }

        switch (operation) {
            case "DELETE":
                // Update delete = new Update().pull(targetPath, value);
                // processResult.setUpdate(delete);
                if (StringUtils.isBlank(targetPath)) {
                    writeModel = new DeleteOneModel<>(criteria);
                } else {
                    writeModel = new UpdateManyModel(criteria, new Document("$unset", targetPath));
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
            if(StringUtils.isNotBlank(source) && StringUtils.isNotBlank(target)){
                criteria.append(target, doc.get(source));
            }
        }

         Object[] matchCondition = (Object[]) mapping.get("match_condition");
         for (Object obj : matchCondition) {
             Map condition = (Map) obj;
             String source = (String) condition.get("source");
             String target = (String) condition.get("target");
             if(StringUtils.isNotBlank(source) && StringUtils.isNotBlank(target) && StringUtils.isNotBlank(targetPath)){
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

}
