package org.example.npm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.util.Iterator;

public class NpmPackageDependencyFunction implements FlatMapFunction<JsonNode, Tuple4<String,String,Long, Integer>> {
    @Override
    public void flatMap(JsonNode json, Collector<Tuple4<String, String,Long, Integer>> out) {
//        System.out.println("json: " + json);

        JsonNode versions = json.get("versions");
        JsonNode times=json.get("time");

//        System.out.println("versions: " + versions);

        if (versions == null) {
            return;
        }

        for (JsonNode v : versions) {
            String nodeVersion=v.get("version").asText();
//            System.out.println("v: " + v);
            String time=times.get(nodeVersion).asText().replace("T"," ").replace("Z","");
 //           System.out.println("time: " + time);
            JsonNode dependencies = v.get("dependencies");
            Timestamp ts = Timestamp.valueOf(time);
            Long timeLong=ts.getTime();
//            System.out.println("timeLong: " + timeLong);
//            System.out.println("dependencies: " + dependencies);

            if (dependencies == null) {
                continue;
            }

            for (Iterator<String> it = dependencies.fieldNames(); it.hasNext(); ) {
                String packageName = it.next();
                String version = dependencies.get(packageName).asText();

//                System.out.println("key: " + key);

                //out.collect(new Tuple2<>(key, 1));
                out.collect(new Tuple4<>(packageName,version,timeLong, 1));
            }
        }

        //out.collect(new Tuple2<>(key, 1));
    }
}
