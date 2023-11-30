package org.example.npm;

import org.apache.flink.api.common.functions.MapFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NpmPackageMapFunction implements MapFunction<String, JsonNode> {
    private transient ObjectMapper jsonParser;

    @Override
    public JsonNode map(String value) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        return jsonParser.readTree(value);
    }
}
