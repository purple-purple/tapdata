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

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.WriteModel;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.Groups;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MongoDBTarget extends BaseTarget {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBTarget.class);

    public static final int DEFAULT_CAPACITY = 2048;

    private final MongoTargetConfigBean mongoTargetConfigBean;
    private MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;
    private ErrorRecordHandler errorRecordHandler;
    private DataGeneratorFactory generatorFactory;

    private TapTransformer transformer;

    public MongoDBTarget(MongoTargetConfigBean mongoTargetConfigBean) {
        this.mongoTargetConfigBean = mongoTargetConfigBean;

    }

    @SuppressWarnings("unchecked")
    @Override
    protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        errorRecordHandler = new DefaultErrorRecordHandler(getContext());

        mongoTargetConfigBean.mongoConfig.init(
                getContext(),
                issues,
                null,
                mongoTargetConfigBean.writeConcern.getWriteConcern()
        );
        if (!issues.isEmpty()) {
            return issues;
        }

        // since no issue was found in validation, the followings must not be null at this point.
        Utils.checkNotNull(mongoTargetConfigBean.mongoConfig.getMongoDatabase(), "MongoDatabase");
        mongoClient = Utils.checkNotNull(mongoTargetConfigBean.mongoConfig.getMongoClient(), "MongoClient");
        mongoCollection = Utils.checkNotNull(mongoTargetConfigBean.mongoConfig.getMongoCollection(), "MongoCollection");

        DataGeneratorFactoryBuilder builder = new DataGeneratorFactoryBuilder(
                getContext(),
                DataFormat.JSON.getGeneratorFormat()
        );
        builder.setCharset(StandardCharsets.UTF_8);
        builder.setMode(Mode.MULTIPLE_OBJECTS);
        generatorFactory = builder.build();
//    getContext().getPipelineConstants()
        transformer = new TapTransformer(mongoTargetConfigBean, issues, getContext());

        try {
            if (StringUtils.isNotBlank(mongoTargetConfigBean.mapping)) {
                Document mapConfig = Document.parse(mongoTargetConfigBean.mapping);
                String mappings = mapConfig.getString("mappings");

                StringBuilder sb = new StringBuilder("{\"mappings\":").append(mappings).append("}");
                Document doc = Document.parse(sb.toString());
                createMongoIndexes((List<Document>) doc.get("mappings"));
            }
        } catch (Exception e) {
            issues.add(getContext().createConfigIssue(
                    Groups.MAPPING.name(),
                    "",
                    Errors.MONGODB_38,
                    e.toString()
                    )
            );
        }

        return issues;
    }

    @Override
    public void destroy() {
        IOUtils.closeQuietly(mongoClient);
        super.destroy();
    }

    @Override
    public void write(Batch batch) throws StageException {
        Iterator<Record> records = batch.getRecords();
        Map<String, List<WriteModel<Document>>> collectionBulkMap = new HashMap<>();
        List<Record> recordList = new ArrayList<>();
        int count = 0;
        while (records.hasNext()) {
            Record record = records.next();
            // for(String attr: record.getHeader().getAttributeNames()){
            //   System.out.println(attr+" -- "+record.getHeader().getAttribute(attr));
            // }
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(DEFAULT_CAPACITY);
                DataGenerator generator = generatorFactory.getGenerator(baos);
                generator.write(record);
                generator.close();
                Document document = Document.parse(new String(baos.toByteArray()));

                // create a write model based on record header
                // if (isNullOrEmpty(record.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE))) {
                //   LOG.error(Errors.MONGODB_15.getMessage(), record.getHeader().getSourceId());
                //   throw new OnRecordErrorException(Errors.MONGODB_15, record.getHeader().getSourceId());
                // }

                Map<String, List<WriteModel<Document>>> map = transformer.processRecord(record, document);
                if (MapUtils.isNotEmpty(map)) {

                    for (Map.Entry<String, List<WriteModel<Document>>> entry : map.entrySet()) {
                        String collectionName = entry.getKey();
                        List<WriteModel<Document>> models = entry.getValue();
                        if (CollectionUtils.isNotEmpty(models)) {
                            if (!collectionBulkMap.containsKey(collectionName)) {
                                collectionBulkMap.put(collectionName, new ArrayList<>());
                            }
                            collectionBulkMap.get(collectionName).addAll(models);
                        }
                    }

                }
//        if(CollectionUtils.isNotEmpty(models)){
//              recordList.add(record);
//              collectionBulkMap.addAll(models);
//        }

            } catch (IOException | StageException | NumberFormatException e) {
                errorRecordHandler.onError(
                        new OnRecordErrorException(
                                record,
                                Errors.MONGODB_13,
                                e.toString(),
                                e
                        )
                );
            }
        }

        if (!collectionBulkMap.isEmpty()) {
            System.out.println("Writing " + collectionBulkMap.size() + " ops ");
            try {
                for (Map.Entry<String, List<WriteModel<Document>>> entry : collectionBulkMap.entrySet()) {
                    String collectionName = entry.getKey();
                    List<WriteModel<Document>> models = entry.getValue();
                    MongoCollection<Document> collection = mongoTargetConfigBean.mongoConfig.getMongoDatabase().getCollection(collectionName);

                    BulkWriteResult bulkWriteResult = collection.bulkWrite(models);

                    if (bulkWriteResult.wasAcknowledged()) {
                        LOG.debug(
                                "Wrote batch with {} inserts, {} updates and {} deletes to {}",
                                bulkWriteResult.getInsertedCount(),
                                bulkWriteResult.getModifiedCount(),
                                bulkWriteResult.getDeletedCount(),
                                collectionName
                        );
                    }
                }
            } catch (MongoException e) {
                for (Record record : recordList) {
                    errorRecordHandler.onError(
                            new OnRecordErrorException(
                                    record,
                                    Errors.MONGODB_17,
                                    e.toString(),
                                    e
                            )
                    );
                }
            }
        }
    }

    private void processOplog() {

    }

    private void validateUniqueKey(String operation, Record record) throws OnRecordErrorException {
        if (mongoTargetConfigBean.uniqueKeyField == null || mongoTargetConfigBean.uniqueKeyField.isEmpty()) {
            LOG.error(
                    Errors.MONGODB_18.getMessage(),
                    operation
            );
            throw new OnRecordErrorException(
                    Errors.MONGODB_18,
                    operation
            );
        }

        if (!record.has(mongoTargetConfigBean.uniqueKeyField)) {
            LOG.error(
                    Errors.MONGODB_16.getMessage(),
                    record.getHeader().getSourceId(),
                    mongoTargetConfigBean.uniqueKeyField
            );
            throw new OnRecordErrorException(
                    Errors.MONGODB_16,
                    record.getHeader().getSourceId(),
                    mongoTargetConfigBean.uniqueKeyField
            );
        }
    }

    private String removeLeadingSlash(String uniqueKeyField) {
        if (uniqueKeyField.startsWith("/")) {
            return uniqueKeyField.substring(1);
        }
        return uniqueKeyField;
    }

    private void createMongoIndexes(List<Document> mappings) {
        for (Document mapping : mappings) {
            String toTable = mapping.getString("to_table");
            String relationship = mapping.getString("relationship");
            if (!"OneOne".equals(relationship)) {
                List<Document> matchConditions = (List<Document>) mapping.get("match_condition");
                if (CollectionUtils.isNotEmpty(matchConditions)) {
                    createIndexes(toTable, matchConditions);
                }
            } else {
                List<Document> joinCondition = (List<Document>) mapping.get("join_condition");
                if (CollectionUtils.isNotEmpty(joinCondition)) {
                    createIndexes(toTable, joinCondition);
                }
            }
        }
    }

    private void createIndexes(String toTable, List<Document> condition) {
        List<String> fieldNames = new ArrayList<>();
        for (Document map : condition) {
            fieldNames.add(map.getString("target"));
        }
        if (CollectionUtils.isNotEmpty(fieldNames)) {
            List<IndexModel> indexModels = new ArrayList<>();
            IndexModel model = new IndexModel(Indexes.ascending(fieldNames));
            model.getOptions().name(UUID.randomUUID().toString());
            indexModels.add(model);
            mongoTargetConfigBean.mongoConfig.getMongoDatabase().getCollection(toTable).createIndexes(indexModels);
        }
    }
}
