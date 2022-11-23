package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
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

        // 1 统计每个用户访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1L);
            }
        }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> event1, Tuple2<String, Long> event2) throws Exception {
                        return Tuple2.of(event1.f0, event1.f1 + event2.f1);
                    }
                });
        // 2 选取当前最活跃用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickByUser.keyBy(data -> "key") // 相当于所有数据扔到一个组（slot）上做运算
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        result.print();

        env.execute();
    }
}
