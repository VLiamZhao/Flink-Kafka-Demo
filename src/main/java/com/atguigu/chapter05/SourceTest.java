package com.atguigu.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // read data from file
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // read from collection
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // read from elements, this method is easy for testing
        DataStreamSource<Event> stream3 = env.fromElements(new Event("Mary", "./home", 1000L), new Event("Bob", "./cart", 2000L));

        // read from socket
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);

        // real company big data source: Kafka
        // need to use flink-connector-kafka dependency

        stream2.print();

        env.execute();
    }
}
