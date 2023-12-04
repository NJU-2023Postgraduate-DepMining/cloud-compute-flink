package org.example.github;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

//将JSON类型GithubRedisMsg转换为Tuple4类型，便于使用keyBy算子与sum算子
public class GitHubFlatMap implements FlatMapFunction<GithubRedisMsg, Tuple4<String, String,Long, Integer>> {

    @Override
    public void flatMap(GithubRedisMsg githubRedisMsg, Collector<Tuple4<String, String,Long, Integer>> collector) throws Exception {
        String packageName = githubRedisMsg.getPackageName();
        String version = githubRedisMsg.getVersion();
        Long timestamp = githubRedisMsg.getTimestamp();
        Integer count = githubRedisMsg.getCount();
        collector.collect(new Tuple4<>(packageName, version,timestamp, count));
    }
}
