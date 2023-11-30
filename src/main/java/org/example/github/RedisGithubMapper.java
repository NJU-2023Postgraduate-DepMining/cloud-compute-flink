package org.example.github;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.protos.GithubKPMsg;

public class RedisGithubMapper implements RedisMapper<GithubRedisMsg> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.RPUSH);
    }

    @Override
    public String getKeyFromData(GithubRedisMsg data) {
        return "github";
    }

    @Override
    public String getValueFromData(GithubRedisMsg data) {
        return data.toString();
    }
}
