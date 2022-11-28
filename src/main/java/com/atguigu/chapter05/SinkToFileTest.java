package com.atguigu.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 4000L),
                new Event("Alice", "./prod?id=201", 5000L),
                new Event("Alice", "./prod?id=202", 6000L)
        );

        StreamingFileSink<String> stringStreamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("utf-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()   // 滚动策略，流式数据达到什么标准可以归档保存，开启新文件存储接下来的新数据
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))  // 隔多长时间滚动一次
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))  // 隔多长时间不活动认为是文件结束，新开启另一个文件
                        .build())
                .build();
        stream
                .map(data -> data.toString())
                .addSink(stringStreamingFileSink);

        env.execute();

    }
}
