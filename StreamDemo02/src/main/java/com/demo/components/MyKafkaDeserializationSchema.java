package com.demo.components;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

// 自定义 Kryo 反序列化类
public class MyKafkaDeserializationSchema<T extends Tuple> implements KafkaDeserializationSchema<Tuple2<String, String>> {

    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (record == null || record.value() == null || record.value().length == 0) {
            throw new IllegalArgumentException("Received empty or null Kafka record");
        }
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String id = topic + "-" + partition + "-" + offset;
        String value = new String(record.value(), StandardCharsets.UTF_8);
        return Tuple2.of(id, value);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
    }
}