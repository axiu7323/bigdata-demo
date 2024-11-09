package com.demo.jobs;


import com.demo.components.MyKafkaDeserializationSchema;
import com.demo.components.FlinkUtil;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

public class UserCount {
    public static void main(String[] args) throws Exception {
        MyKafkaDeserializationSchema<Tuple> kryoDeserializationSchema = new MyKafkaDeserializationSchema<>();

        // 创建 Kafka 数据流，传递正确的泛型类型
        DataStream<Tuple2<String, String>> dataStream = FlinkUtil.createKafkaStream(args,kryoDeserializationSchema );

        // 打印 Kafka 消息以进行调试
        dataStream.print();

        // 执行 Flink 任务
        FlinkUtil.env.execute("Kafka to Flink Streaming Job");
    }
}
