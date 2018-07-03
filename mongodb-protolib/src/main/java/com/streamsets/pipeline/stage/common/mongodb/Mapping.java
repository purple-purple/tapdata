package com.streamsets.pipeline.stage.common.mongodb;

import java.util.List;
import java.util.Map;

public class Mapping {

    private String from_table;

    private String to_table;

    private String relationship;

    private List<String> pk;

    private String embed_behavior;

    private String target_path;

    private List<Map<String, String>> join_condition;

    private List<Map<String, String>> match_condition;

    public Mapping() {
    }

    public Mapping(String from_table, String to_table, List<String> pks, List<Map<String, String>> join_condition) {
        this.from_table = from_table;
        this.to_table = to_table;
        this.pk = pks;
        this.join_condition = join_condition;
    }

    public String getFrom_table() {
        return from_table;
    }

    public void setFrom_table(String from_table) {
        this.from_table = from_table;
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

    public List<String> getPk() {
        return pk;
    }

    public void setPk(List<String> pk) {
        this.pk = pk;
    }

    public String getEmbed_behavior() {
        return embed_behavior;
    }

    public void setEmbed_behavior(String embed_behavior) {
        this.embed_behavior = embed_behavior;
    }

    public String getTarget_path() {
        return target_path;
    }

    public void setTarget_path(String target_path) {
        this.target_path = target_path;
    }

    public List<Map<String, String>> getJoin_condition() {
        return join_condition;
    }

    public void setJoin_condition(List<Map<String, String>> join_condition) {
        this.join_condition = join_condition;
    }

    public List<Map<String, String>> getMatch_condition() {
        return match_condition;
    }

    public void setMatch_condition(List<Map<String, String>> match_condition) {
        this.match_condition = match_condition;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Mapping{");
        sb.append("from_table='").append(from_table).append('\'');
        sb.append(", to_table='").append(to_table).append('\'');
        sb.append(", relationship='").append(relationship).append('\'');
        sb.append(", pk=").append(pk);
        sb.append(", embed_behavior='").append(embed_behavior).append('\'');
        sb.append(", target_path='").append(target_path).append('\'');
        sb.append(", join_condition=").append(join_condition);
        sb.append(", match_condition=").append(match_condition);
        sb.append('}');
        return sb.toString();
    }
}
