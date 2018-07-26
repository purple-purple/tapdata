package com.streamsets.pipeline.lib.jdbc.load.schema;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MappingBean {

    private final static Logger LOG = LoggerFactory.getLogger(MappingBean.class);

    private final static String MAPPINGS = "mappings";
    private final static String JOINCONDITION = "join_condition";
    private final static String SOURCE = "source";
    private final static String TARGET = "target";
    private final static String FROM_TABLE = "from_table";
    private final static String TO_TABLE = "to_table";
    private final static String CUSTOM_SQL = "custom_sql";
    private final static String OFFSET = "offset";
    private final static String RELATIONSHIP = "relationship";
    private final static String TARGET_PATH = "target_path";

    private final static String PARSE_ERROR = "Parse mapping error:%s \n mapping: %s";

    public MappingBean(String mapping, boolean isParse) {
        this.mapping = mapping;

        if (isParse) {
            parseMapping();
        }
    }

    private void parseMapping() {
        if (StringUtils.isNotBlank(mapping)) {
            try {
                ObjectMapper mapper = new ObjectMapper();

                JsonNode jn = mapper.readTree(mapping);

                String mappingStr = jn.path(MAPPINGS).asText();

                if (StringUtils.isNotBlank(mappingStr)) {

                    jn = mapper.readTree(mappingStr);

                    for (JsonNode jsonNode : jn) {
                        List<MappingFeild> fields = new ArrayList<>();

                        JsonNode joinCondition = jsonNode.path(JOINCONDITION);
                        for (JsonNode childNode : joinCondition) {
                            String source = childNode.path(SOURCE).asText();
                            String target = childNode.path(TARGET).asText();
                            fields.add(new MappingFeild(source, target));
                        }

                        mappings.add(new MappingInfo(
                                jsonNode.path(FROM_TABLE).asText(),
                                jsonNode.path(CUSTOM_SQL).asText(),
                                jsonNode.path(OFFSET).asText(),
                                jsonNode.path(TO_TABLE).asText(),
                                jsonNode.path(RELATIONSHIP).asText(),
                                jsonNode.path(TARGET_PATH).asText(),
                                fields
                        ));
                    }
                }
            } catch (IOException e) {
                mappings.clear();
                LOG.error(String.format(PARSE_ERROR, e.toString(), mapping));
            }
        }
    }

    private String mapping;
    private List<MappingInfo> mappings = new ArrayList<>();

    public String getMapping() {
        return mapping;
    }

    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    public List<MappingInfo> getMappings() {
        return mappings;
    }

    public void setMappings(List<MappingInfo> mappings) {
        this.mappings = mappings;
    }

    public class MappingInfo {
        private String from_table;
        private String custom_sql;
        private String offset;
        private String to_table;
        private String relationship;
        private String target_path;
        private List<MappingFeild> join_condition = new ArrayList<>();

        public MappingInfo(String from_table, String custom_sql, String offset, String to_table, String relationship, String target_path, List<MappingFeild> join_condition) {
            this.from_table = from_table;
            this.custom_sql = custom_sql;
            this.offset = offset;
            this.to_table = to_table;
            this.relationship = relationship;
            this.target_path = target_path;
            this.join_condition = join_condition;
        }

        public List<MappingFeild> getJoin_condition() {
            return join_condition;
        }

        public void setJoin_condition(List<MappingFeild> join_condition) {
            this.join_condition = join_condition;
        }

        public String getFrom_table() {
            return from_table;
        }

        public void setFrom_table(String from_table) {
            this.from_table = from_table;
        }

        public String getCustom_sql() {
            return custom_sql;
        }

        public void setCustom_sql(String custom_sql) {
            this.custom_sql = custom_sql;
        }

        public String getOffset() {
            return offset;
        }

        public void setOffset(String offset) {
            this.offset = offset;
        }

        public String getTo_table() {
            return to_table;
        }

        public void setTo_table(String to_table) {
            this.to_table = to_table;
        }

        public String getRelationship() {
            return relationship;
        }

        public void setRelationship(String relationship) {
            this.relationship = relationship;
        }

        public String getTarget_path() {
            return target_path;
        }

        public void setTarget_path(String target_path) {
            this.target_path = target_path;
        }
    }

    public class MappingFeild {
        private String source;
        private String target;

        public MappingFeild(String source, String target) {
            this.source = source;
            this.target = target;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }
    }
}
