package com.gaosen.demo

//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.kafka.HasOffsetRanges
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author gaosen
 * @Date 2022/4/24 17:24
 * @Email gaosen@tuyoogame.com
 * @Description
 *
 */
object KafkaWordCount {

//    def main(args: Array[String]): Unit = {
//        val conf = new SparkConf().setAppName("SparkStreamWordCount").setMaster("local[*]")
//        val sc = new SparkContext(conf)
//        sc.setLogLevel("ERROR")
//        val ssc = new StreamingContext(sc, Seconds(10))
//        ssc.checkpoint(".")
//
//        //创建连接kafka服务参数
//        val kafkaParams: Map[String, Object] = Map[String, Object](
//            "bootstrap.servers" -> "localhost:9092",
//            "key.deserializer" -> classOf[StringDeserializer],
//            "value.deserializer" -> classOf[StringDeserializer],
//            "group.id" -> "groupA",
//            "auto.offset.reset" -> "latest",
//            "enable.auto.commit" -> (true: java.lang.Boolean)
//        )
//
//        val topics = Array("wordsender")
//        val stream = KafkaUtils.createDirectStream[String, String](
//            ssc,
//            PreferConsistent,
//            Subscribe[String, String](topics, kafkaParams)
//        )
//
//        stream.foreachRDD(rdd => {
//            val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//            val maped: RDD[(String, String)] = rdd.map(record => (record.key, record.value))
//            val lines = maped.map(_._2)
//            val words = lines.flatMap(_.split(" "))
//            val pair = words.map(x => (x,1))
//            val wordCounts = pair.reduceByKey(_+_)
//            wordCounts.foreach(println)
//        })
//
//        ssc.start
//        ssc.awaitTermination
//
//    }

}
