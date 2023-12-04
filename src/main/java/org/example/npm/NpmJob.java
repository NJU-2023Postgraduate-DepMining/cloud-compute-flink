package org.example.npm;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.example.job.Job;
import org.example.npm.NpmPackageDependencyFunction;
import org.example.npm.NpmPackageMapFunction;
import org.example.npm.RedisNpmMapper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class NpmJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamFormat<String> format = new TextLineInputFormat();

        // 从文件读取数据
        FileSource<String> source = FileSource.forRecordStreamFormat(format,
//                        new Path("file:///data/npm_all_json.txt"))
//                        new Path("file:///data/b.txt"))
                             new Path(Job.npmPath))
                .build();

        DataStream<String> lines = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "NpmFileSource");

        // 将每一行解析成 JSON 对象
        DataStream<JsonNode> packages = lines
                .map(new NpmPackageMapFunction())
                .name("NpmPackageMapFunction");

        DataStream<Tuple4<String,String,Long, Integer>> counts = packages
                .flatMap(new NpmPackageDependencyFunction());


        counts.addSink(new NpmCHSink()).setParallelism(1);
        counts.print();
//        DataStream<Tuple2<String, Integer>> sum = counts.keyBy(x -> x.f0).sum(1);
//        sum.print();

//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
//                .setHost("redis")
//                .setPort(6379)
//                .build();
//
//        counts.addSink(new RedisSink<>(conf, new RedisNpmMapper()));



//        //        output to file
//        counts.sinkTo(FileSink.forRowFormat(new Path("file:///data/out.txt"),
//                new NpmPackageDependencySinkFunction()).build());


        env.execute("Npm Job");
    }
}