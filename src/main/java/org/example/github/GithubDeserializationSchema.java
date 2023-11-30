package org.example.github;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.protos.GithubKPMsg;

import java.io.IOException;

public class GithubDeserializationSchema implements DeserializationSchema<GithubKPMsg> {

    @Override
    public GithubKPMsg deserialize(byte[] message) throws IOException {
        return GithubKPMsg.parseFrom(message);
    }

    @Override
    public boolean isEndOfStream(GithubKPMsg nextElement) {
        return false;
    }

    @Override
    public TypeInformation<GithubKPMsg> getProducedType() {
        return TypeInformation.of(GithubKPMsg.class);
    }
}