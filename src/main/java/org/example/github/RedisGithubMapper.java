package org.example.github;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.protos.GithubKPMsg;

public class RedisGithubMapper implements RedisMapper<String> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.INCRBY);
    }

    @Override
    public String getKeyFromData(String data) {
        return data;
    }

    @Override
    public String getValueFromData(String data) {
        return "1";
    }
}
