package gstore.api.result;

import com.fasterxml.jackson.databind.JsonNode;
import gstore.util.JsonUtil;

import java.util.ArrayList;
import java.util.List;

public class Result {

    private int statusCode;

    private String statusMsg;

    private long queryTime;

    private String tid;

    private List<String> vars;

    private List<Record> records;

    private JsonNode jsonNode;

    public Result() {
        vars = new ArrayList<>();
        records = new ArrayList<>();
    }

    public static Result instance() {
        return new Result();
    }

    public boolean success() {
        return statusCode == 0;
    }

    public String statusMsg() {
        return statusMsg;
    }

    public List<String> vars() {
        return vars;
    }

    public List<Record> records(){ return records;}

    public long queryTime() { return queryTime; }

    public String tid() { return tid; }

    public Record single() {
        if (!records.isEmpty()) {
            return records.get(0);
        } else {
            return null;
        }
    }

    public List<Record> list() {
        return records;
    }

    public void consume() {
        //do nothing
    }

    public String get(String key) {
        JsonNode json =  jsonNode.get(key);
        if (json != null) {
            return json.toString();
        }
        return null;
    }
    public  String getNum(String key) {
        JsonNode json =  jsonNode.get(key);
        if (json != null) {
            return json.asText();
        }
        return null;
    }

    public Result build(String str) throws Exception{
        jsonNode = JsonUtil.toJson(str);
        if (jsonNode == null) {
            throw new Exception("parse json string error.");
        }
        if (jsonNode.has("StatusCode")) {
            this.statusCode = jsonNode.get("StatusCode").asInt();
        }
        if (jsonNode.has("StatusMsg")) {
            this.statusMsg = jsonNode.get("StatusMsg").asText();
        }
        if (jsonNode.has("QueryTime")) {
            this.queryTime = jsonNode.get("QueryTime").asLong();
        }
        if (jsonNode.has("TID")) {
            this.tid = jsonNode.get("TID").asText();
        }
        String[] vars = null;
        if (jsonNode.has("head")) {
            JsonNode headNode = jsonNode.get("head");
            if (headNode.has("vars") && headNode.get("vars").isArray()) {
                JsonNode varNode = headNode.get("vars");
                vars = new String[varNode.size()];
                for (int i=0; i<varNode.size(); i++) {
                    vars[i] = varNode.get(i).asText();
                    this.vars.add(vars[i]);
                }
            }
        }
        if (jsonNode.has("results") && vars != null) {
            JsonNode dataNode = jsonNode.get("results");
            if (dataNode.has("bindings") && dataNode.get("bindings").isArray()) {
                JsonNode bindingsNode = dataNode.get("bindings");
                for (JsonNode item : bindingsNode) {
                    if (item.isObject()) {
                        Value[] values = new Value[vars.length];
                        for (int i = 0; i < vars.length; i++) {
                            if (item.has(vars[i])) {
                                values[i] =  JsonUtil.toBean(item.get(vars[i]), Value.class);
                            }
                        }
                        Record record = new Record(vars, values);
                        this.records.add(record);
                    }
                }
            }
        }
        return this;
    }
}
