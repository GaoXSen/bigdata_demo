package com.gaosen.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author gaosen
 * @Date 2022/4/24 9:55
 * @Email gaosen@tuyoogame.com
 * @Description
 *
 */
object SparkWordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)
        val lineRDD: RDD[String] = sc.textFile("D:\\test.txt")
        val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
        val word20neRDD: RDD[(String, Int)] = wordRDD.map((_,1))
        val word2SumRDD: RDD[(String, Int)] = word20neRDD.reduceByKey(_+_)
        val result:Array[(String, Int)] = word2SumRDD.collect()
//        word2SumRDD.saveAsTextFile("D:\\result\\1.txt")
        result.foreach(println)
        sc.stop()

    }
}
