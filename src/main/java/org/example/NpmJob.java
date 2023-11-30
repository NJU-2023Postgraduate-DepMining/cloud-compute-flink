package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.example.npm.NpmPackageDependencyFunction;
import org.example.npm.NpmPackageMapFunction;
import org.example.npm.RedisNpmMapper;


public class NpmJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamFormat<String> format = new TextLineInputFormat();

        // 从文件读取数据
        FileSource<String> source = FileSource.forRecordStreamFormat(format,
                        new Path("file:///data/npm_all_json.txt"))
//                        new Path("file:///data/b.txt"))
                .build();

        DataStream<String> lines = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "NpmFileSource");

        // 将每一行解析成 JSON 对象
        DataStream<JsonNode> packages = lines
                .map(new NpmPackageMapFunction())
                .name("NpmPackageMapFunction");

        DataStream<Tuple2<String, Integer>> counts = packages
                .flatMap(new NpmPackageDependencyFunction());

//        DataStream<Tuple2<String, Integer>> sum = counts.keyBy(x -> x.f0).sum(1);
//        sum.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("redis")
                .setPort(6379)
                .build();

        counts.addSink(new RedisSink<>(conf, new RedisNpmMapper()));


//        //        output to file
//        counts.sinkTo(FileSink.forRowFormat(new Path("file:///data/out.txt"),
//                new NpmPackageDependencySinkFunction()).build());


        env.execute("Npm Job");
    }
}