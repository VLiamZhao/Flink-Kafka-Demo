package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));

        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());

        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getUser();
            }
        });

        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.getUser());

        result3.print();

        env.execute();
    }

    // 自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.getUser();
        }
    }

}
