package com.atguigu.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
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

        stream.map(new MyRichMapper()).setParallelism(2).print();

        env.execute();
    }

    public static class MyRichMapper extends RichMapFunction<Event, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open 生命周期： " + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public Integer map(Event event) throws Exception {
            return event.getUrl().length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close 生命周期： " + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }
    }
}
