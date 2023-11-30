package org.example.npm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;

public class NpmPackageDependencyFunction implements FlatMapFunction<JsonNode, Tuple2<String, Integer>> {
    @Override
    public void flatMap(JsonNode json, Collector<Tuple2<String, Integer>> out) {
//        System.out.println("json: " + json);

        JsonNode versions = json.get("versions");

//        System.out.println("versions: " + versions);

        if (versions == null) {
            return;
        }

        for (JsonNode v : versions) {
//            System.out.println("v: " + v);

            JsonNode dependencies = v.get("dependencies");

//            System.out.println("dependencies: " + dependencies);

            if (dependencies == null) {
                continue;
            }

            for (Iterator<String> it = dependencies.fieldNames(); it.hasNext(); ) {
                String key = it.next();

//                System.out.println("key: " + key);

                out.collect(new Tuple2<>(key, 1));
            }
        }

        //out.collect(new Tuple2<>(key, 1));
    }
}
