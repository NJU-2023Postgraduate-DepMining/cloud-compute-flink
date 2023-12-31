package org.example.github;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.Tumble;
import org.example.job.Job;
import org.example.protos.GithubKPMsg;
import org.apache.commons.cli.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GithubJob {
    public static void main(String[] args) throws Exception {
        Options options = getOptions();

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        String kafkaAddress = cmd.getOptionValue("kafka", Job.kafka);
        String redisAddress = cmd.getOptionValue("redis_address", "redis");
        String kafkaTopic = cmd.getOptionValue("topic", "topic_github");
        int redisPort = Integer.parseInt(cmd.getOptionValue("redisPort", "6379"));


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // https://stackoverflow.com/questions/70048053/apache-flink-fails-with-kryoexception-when-serializing-pojo-class
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);


        KafkaSource<GithubKPMsg> source = KafkaSource.<GithubKPMsg>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(kafkaTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GithubDeserializationSchema())
                .build();

        DataStream<GithubKPMsg> s = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "GithubKafkaSource");

        DataStream<GithubRedisMsg> dep = s.flatMap(new GithubDependencyMapFunction());

        DataStream<Tuple4<String, String,Long, Integer>> resDS = dep.flatMap(new GitHubFlatMap());

        resDS.addSink(new GithubCHSink()).setParallelism(1);

//        s.print();
//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
//                .setHost(redisAddress)
//                .setPort(redisPort)
//                .build();

//        dep.addSink(new RedisSink<>(conf, new RedisGithubMapper()));

        env.execute("Github Job");
    }

    private static Options getOptions() {
        Options options = new Options();
        Option input = new Option("kafka", true, "kafka address, default: kafka:9092");
        input.setRequired(false);
        options.addOption(input);

        Option topic = new Option("topic", true, "kafka topic, default: topic_github");
        topic.setRequired(false);
        options.addOption(topic);

        Option r = new Option("redis_address", true, "redis address, default: redis");
        r.setRequired(false);
        options.addOption(r);

        Option redisPort = new Option("redisPort", true, "redis port, default: 6379");
        redisPort.setRequired(false);
        options.addOption(redisPort);
        return options;
    }
}
