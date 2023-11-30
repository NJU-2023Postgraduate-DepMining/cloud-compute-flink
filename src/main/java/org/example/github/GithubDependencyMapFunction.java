package org.example.github;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.example.protos.Dependency;
import org.example.protos.GithubKPMsg;

import java.util.List;

public class GithubDependencyMapFunction implements FlatMapFunction<GithubKPMsg, GithubRedisMsg> {
    @Override
    public void flatMap(GithubKPMsg value, Collector<GithubRedisMsg> out) throws Exception {
//        String repo = value.getRepo();
        List<Dependency> dependencies = value.getDependenciesList();
        for (Dependency dependency : dependencies) {
            out.collect(new GithubRedisMsg(dependency.getPackage(), dependency.getVersion(), value.getTimestamp()));
        }
    }
}
