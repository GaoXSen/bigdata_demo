package com.gaosen.demo

//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * @Author gaosen
 * @Date 2022/4/25 14:56
 * @Email gaosen@tuyoogame.com
 * @Description
 *
 */
object FlinkWordCount {

//    def main(args: Array[String]): Unit = {
//        // 获取执行器的环境
//        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//        env.enableCheckpointing(5000)
//
//        // kafka配置
//        val properties = new Properties()
//        properties.setProperty("bootstrap.servers","localhost:9092")
//        properties.setProperty("zookeeper.connect", "localhost:2181")
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//        properties.setProperty("auto.offset.reset", "earliest")
//        properties.setProperty("enable.auto.commit", "true")
//        properties.setProperty("group.id","groupB")
//
//        // 消费Kafka数据
//        val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("kafka_test", new SimpleStringSchema(), properties))
//        val stream2 = stream.map(_.split("")).flatMap(_.toSeq).map((_, 1)).keyBy(0).sum(1)
//        stream2.addSink( tup => {
//
//        })
//
//
//    }

}
