package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;

public class NpmPackageDependencyFunction implements FlatMapFunction<JsonNode, Tuple2<String, Integer>> {
    @Override
    public void flatMap(JsonNode json, Collector<Tuple2<String, Integer>> out) {
        JsonNode dependencies = json.get("dependencies");

        if (dependencies == null) {
            return;
        }

        dependencies.fieldNames().forEachRemaining(name -> {
            out.collect(new Tuple2<>(name, 1));
        });

        //out.collect(new Tuple2<>(key, 1));
    }
}
