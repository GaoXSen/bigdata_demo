package com.gaosen.demo

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Author gaosen
 * @Date 2022/4/19 14:34
 * @Email gaosen@tuyoogame.com
 * @Description
 *
 */
object SparkStreamDemo {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SparkStreamWordCount").setMaster("local[*]")
        //流处理的上下文类
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint(".")

        //创建连接kafka服务参数
        val kafkaParams: Map[String, Object] = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "groupA",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> "false"
        )

        //topic 列表
        val topics = Array("kafka_test")

        /**
         * createDirectStream: 主动拉取数据
         */
        val linesDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        /**
         * kafka 是一个key value格式的，默认key为null，一般用不上
         */
        linesDS
            .map(record => (record.key(), record.value()))
            .map(_._2)
            .flatMap(_.split(""))
            .map((_, 1))
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Some(seq.sum + opt.getOrElse(0)))
            .print()

        ssc.start()
        ssc.awaitTermination()
        ssc.stop()




    }

}
