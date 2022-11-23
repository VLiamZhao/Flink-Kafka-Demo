package com.atguigu.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 4000L),
                new Event("Alice", "./prod?id=201", 5000L),
                new Event("Alice", "./prod?id=202", 6000L)
        );

        // 按键分组之后进行聚合，提取当前用户最后一次访问
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) {
                return event.getUser();
            }
        }).max("timestamp").print("max: ");

        stream.keyBy(data -> data.getUser()).maxBy("timestamp").print("maxBy: ");

        env.execute();
    }
}
