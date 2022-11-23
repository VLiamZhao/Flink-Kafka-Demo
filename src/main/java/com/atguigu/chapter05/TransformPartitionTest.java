package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class TransformPartitionTest {
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

        // 随机
//        stream.shuffle().print().setParallelism(4);

        // 轮询
        stream.rebalance().print().setParallelism(4);

        // rescale
        env.addSource(new ParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 0; i < 8; i++) {
                //将奇偶数分别发送到0和1并行分区
//                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        sourceContext.collect(i);
//                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);

        env.execute();
    }
}
