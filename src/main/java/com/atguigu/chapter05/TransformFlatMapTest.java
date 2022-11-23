package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );

//        stream.flatMap(new MyFlatMap()).print();
        stream.flatMap((event, collector) -> {
            if (event.getUser().equals("Mary")) {
                collector.collect(event.getUrl());
            } else if (event.getUser().equals("Bob")) {
                collector.collect(event.getUser());
                collector.collect(event.getUrl());
                collector.collect(event.getTimestamp().toString());
            }
        })
                .returns(new TypeHint<Object>() {})
                .print("2");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.getUser());
            collector.collect(event.getUrl());
            collector.collect(event.getTimestamp().toString());
        }
    }
}
