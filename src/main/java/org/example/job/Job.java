package org.example.job;

import com.fasterxml.jackson.databind.JsonNode;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.github.*;
import org.example.npm.NpmCHSink;
import org.example.npm.NpmPackageDependencyFunction;
import org.example.npm.NpmPackageMapFunction;
import org.example.protos.GithubKPMsg;

public class Job {
    public static final String url="jdbc:ch://172.29.4.74:30012/cloud";
    //public static final String url = "jdbc:ch://localhost:8123/cloud";
    //public static final String url = "jdbc:ch://172.29.4.74:30012/test";
    public static final String kafka = "kafka.kafka:9092";

    public static final String npmPath="file:///pool/npm_all.txt";


    public static void main(String[] args) throws Exception {
        Options options = getOptions();

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        StreamFormat<String> format = new TextLineInputFormat();

        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        String kafkaAddress = cmd.getOptionValue("kafka", kafka);
        String kafkaTopic = cmd.getOptionValue("topic", "topic_github");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // https://stackoverflow.com/questions/70048053/apache-flink-fails-with-kryoexception-when-serializing-pojo-class
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);


        KafkaSource<GithubKPMsg> githubSource = KafkaSource.<GithubKPMsg>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(kafkaTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GithubDeserializationSchema())
                .build();

        DataStream<GithubKPMsg> s = env.fromSource(githubSource,
                WatermarkStrategy.noWatermarks(),
                "GithubKafkaSource");

        DataStream<GithubRedisMsg> dep = s.flatMap(new GithubDependencyMapFunction());

        DataStream<Tuple4<String, String,Long, Integer>> githubResDS = dep.flatMap(new GitHubFlatMap());

        githubResDS.addSink(new GithubCHSink());
        githubResDS.print();

        FileSource<String> npmSource = FileSource.forRecordStreamFormat(format,
//                        new Path("file:///data/npm_all_json.txt"))
//                        new Path("file:///data/b.txt"))
                        new Path(npmPath))
                .build();

        DataStream<String> npmLines = env.fromSource(npmSource,
                WatermarkStrategy.noWatermarks(),
                "NpmFileSource");

        // 将每一行解析成 JSON 对象
        DataStream<JsonNode> npmPackages = npmLines
                .map(new NpmPackageMapFunction())
                .name("NpmPackageMapFunction");

        DataStream<Tuple4<String,String,Long, Integer>> npmResDs = npmPackages
                .flatMap(new NpmPackageDependencyFunction());

        npmResDs.addSink(new NpmCHSink());
        npmResDs.print();


//        s.print();
//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
//                .setHost(redisAddress)
//                .setPort(redisPort)
//                .build();

//        dep.addSink(new RedisSink<>(conf, new RedisGithubMapper()));

        env.execute("Job");
    }

    private static Options getOptions() {
        Options options = new Options();
        Option input = new Option("kafka", true, "kafka address, default: kafka:9092");
        input.setRequired(false);
        options.addOption(input);

        Option topic = new Option("topic", true, "kafka topic, default: topic_github");
        topic.setRequired(false);
        options.addOption(topic);

        return options;
    }
}
