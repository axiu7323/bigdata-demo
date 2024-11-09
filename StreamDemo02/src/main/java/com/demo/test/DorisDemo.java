package com.demo.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DorisDemo {
    public static void main(String[] args) throws Exception {
        // 创建表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 配置检查点
        env.enableCheckpointing(60000); // 每 60 秒进行一次检查点

        // 设置检查点的模式（精确一次或者至少一次）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 检查点的超时时间为 60 秒

        // 设置两个检查点之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000); // 两次检查点之间的最小时间间隔为 30 秒

        // 设置可以同时进行的最大检查点数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 当作业取消时保留检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 如果需要，可以设置检查点的存储位置（例如文件系统或者对象存储）
        // env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));

        // 你的数据源和逻辑代码...

        // 创建连接器表（MySQL Source）
        String jdbcSource = "CREATE TABLE flink_jdbc_source (\n" +
                " id INT,\n" +
                " name VARCHAR(255),\n" +
                " age INT,\n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://node01:3306/flinkondrois?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8&useSSL=false',\n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'table-name' = 'user'\n" +
                ");";
        tableEnv.executeSql(jdbcSource);

        // 创建连接器表（Doris Sink）
        String dorisSink = "CREATE TABLE flink_doris_sink (\n" +
                " id INT,\n" +
                " name VARCHAR(255),\n" +
                " age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'doris',\n" +
                " 'fenodes' = 'node01:8030',\n" +
                " 'table.identifier' = 'test_db.user',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'sink.label-prefix' = 'doris_label'\n" +
                ");";
        tableEnv.executeSql(dorisSink);

        // 插入数据
        tableEnv.executeSql("INSERT INTO flink_doris_sink SELECT * FROM flink_jdbc_source");

        // 查询数据
        tableEnv.sqlQuery("SELECT * FROM flink_doris_sink").execute().print();

        env.execute("MySQL to Doris Data Migration");
    }
}
