package org.example.npm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisNpmMapper implements RedisMapper<Tuple2<String, Integer>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.INCRBY);
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> data) {
        return data.f1.toString();
    }
}
