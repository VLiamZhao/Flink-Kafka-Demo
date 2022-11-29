package com.atguigu.chapter05;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToMySql {
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

        stream.addSink(JdbcSink.sink(
                "INSERT INTO clicks (user, url) VALUES (?, ?)",
                ((statement, event) -> {
                    statement.setString(1, event.getUser());
                    statement.setString(2, event.getUrl());
                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root") // 这样写死当然不安全，不推荐
                        .build()
        ));
    }
}
