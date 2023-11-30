package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.Path;


public class NpmJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamFormat<String> format = new TextLineInputFormat();

        // reads the contents of a file from a file stream.
        // /data/npm_all_json.txt
        final FileSource<String> source = FileSource.forRecordStreamFormat(format,
//                new Path("file:///data/npm_all_json.txt"))
                        new Path("file:///data/b.txt"))
                .build();

        DataStream<String> lines = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "NpmFileSource");

        // 将每一行解析成 JSON 对象
        DataStream<JsonNode> packages = lines
                .map(new NpmPackageMapFunction())
                .name("NpmPackageMapFunction");

        packages.print();

        DataStream<Tuple2<String, Integer>> counts = packages
                .flatMap(new NpmPackageDependencyFunction());


        counts.print();

        DataStream<Tuple2<String, Integer>> sum = counts.keyBy(x -> x.f0).sum(1);

        sum.print();


        //        output to file
        sum.sinkTo(FileSink.forRowFormat(new Path("file:///data/out.txt"),
                new NpmPackageDependencySinkFunction()).build());


        env.execute("Npm Job");
    }
}