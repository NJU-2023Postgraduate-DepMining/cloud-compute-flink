package org.example;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.OutputStream;

public class NpmPackageDependencySinkFunction implements Encoder<Tuple2<String, Integer>> {
    @Override
    public void encode(Tuple2<String, Integer> in, OutputStream outputStream) throws IOException {
        outputStream.write((in.f0 + " " + in.f1 + "\n").getBytes());
    }
}
