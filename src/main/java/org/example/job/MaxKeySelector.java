package org.example.job;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;


public class MaxKeySelector implements KeySelector<Tuple4<String, String, Long, Integer>, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> getKey(Tuple4<String, String, Long, Integer> t) throws Exception {
        return Tuple2.of(t.f0, t.f1);

    }
}
