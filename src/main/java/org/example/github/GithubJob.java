package org.example.github;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.example.npm.RedisNpmMapper;
import org.example.protos.GithubKPMsg;

public class GithubJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // https://stackoverflow.com/questions/70048053/apache-flink-fails-with-kryoexception-when-serializing-pojo-class
        Class<?> unmodColl = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodColl, UnmodifiableCollectionsSerializer.class);


        KafkaSource<GithubKPMsg> source = KafkaSource.<GithubKPMsg>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("topic_github")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GithubDeserializationSchema())
                .build();

        DataStream<GithubKPMsg> s =env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "GithubKafkaSource");

        DataStream<String> dep = s.flatMap(new GithubDependencyMapFunction());

//        s.print();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("redis")
                .setPort(6379)
                .build();

        dep.addSink(new RedisSink<>(conf, new RedisGithubMapper()));

        env.execute("Github Job");
    }
}
