package com.gaosen.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author gaosen
 * @Date 2022/4/22 11:14
 * @Email gaosen@tuyoogame.com
 * @Description
 *
 */
object SparkStreamWordCount {

//    1、监控本地文件夹下的文件信息

    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("SparkStreamWordCount").setMaster("local[2]")//这里指在本地运行，2个线程，一个监听，一个处理数据
        // Create the context
        val ssc = new StreamingContext(sparkConf, Seconds(5))// 时间划分为5秒

        //设置检查点
        ssc.checkpoint(".")

        val lines = ssc.textFileStream("D:\\test.txt")
        val words = lines.flatMap(_.split(""))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()
        ssc.start()
        ssc.awaitTermination()
    }




}
