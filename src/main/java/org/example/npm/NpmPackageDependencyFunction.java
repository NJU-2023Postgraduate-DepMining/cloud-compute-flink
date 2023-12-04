package org.example.npm;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

public class NpmPackageDependencyFunction implements FlatMapFunction<JsonNode, Tuple4<String, String, Long, Integer>> {
    @Override
    public void flatMap(JsonNode json, Collector<Tuple4<String, String, Long, Integer>> out) {
        String time = json.get("timestamp").asText().replace("T", " ").replace("Z", "");

        JsonNode dependencies = json.get("dependencies");
        Timestamp ts = Timestamp.valueOf(time);
        Long timeLong = ts.getTime();
        for (JsonNode dependency : dependencies) {
            String packageName = dependency.get("package").asText();
            String version = dependency.get("version").asText();
            out.collect(new Tuple4<>(packageName, version, timeLong, 1));
        }
    }

}

