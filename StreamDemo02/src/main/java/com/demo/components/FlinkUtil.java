package com.demo.components;

import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FlinkUtil {

    // 执行环境
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    public static ParameterTool parameterTool = null;

    // 读取配置文件并返回 Flink 执行环境的方法
    public static <T> DataStream<T> createKafkaStream(String[] args, KafkaDeserializationSchema<T> deserializationSchema) {

        // Flink 内置读取文件对象
        try {
             parameterTool = loadConfiguration(args);
            // 将文件参数注册到环境中
            env.getConfig().setGlobalJobParameters(parameterTool);

            // checkpoint 配置文件
            long chkInterval = parameterTool.getLong("checkpoint.interval", 60000L); // 增加默认值，防止配置缺失
            String chkPath = parameterTool.get("checkpoint.path", "file:///tmp/flink/checkpoints"); // 增加默认值

            // Flink 执行 checkpoint 并设置恰好一次语义
            env.enableCheckpointing(chkInterval, CheckpointingMode.EXACTLY_ONCE);
            env.setStateBackend(new RocksDBStateBackend(chkPath, false));
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            // kafka topic
            List<String> topics = Arrays.asList(parameterTool.get("kafka.input.topics").split(","));

            // 获取 Kafka 连接的相关配置参数，这里是通过 parameterTool.getProperties() 方法直接从配置文件中提取的所有配置项。
            Properties properties = parameterTool.getProperties();

            // 创建 Kafka 消费者组
            FlinkKafkaConsumer<T> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topics, deserializationSchema, properties);
            // 让 Flink 管偏移量
            flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

            // 将 Kafka 消费者作为数据源添加到 Flink 的执行环境中，并返回该数据流。
            // 这里使用 env.addSource 来将 Kafka 消费者作为流处理作业的输入源。
            return env.addSource(flinkKafkaConsumer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static ParameterTool loadConfiguration(String[] args) throws IOException {
        if (args.length > 0) {
            return ParameterTool.fromArgs(args)
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromPropertiesFile(args[0]));
        } else {
            String defaultConfigPath = "src/main/resources/conf.properties";
            return ParameterTool.fromSystemProperties()
                    .mergeWith(ParameterTool.fromPropertiesFile(defaultConfigPath));
        }
    }
}