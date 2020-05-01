package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 窗口操作
  */
object SparkStreaming06_Window {

  def main(args: Array[String]): Unit = {

    /*// Scala中的窗口
    val ints: List[Int] = List(1, 2, 3, 4, 5, 6)

    // 滑动窗口函数
    val intses: Iterator[List[Int]] = ints.sliding(3, 3)

    for (list <- intses) {
      println(list.mkString(", "))
    }*/

    // SparkStreaming的窗口

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming06_Window")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从Kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop01:2181",
      "byhq",
      Map("byhq" -> 3)
    )
    // 窗口大小为采集周期的整数倍，窗口滑动的步长也为采集周期的整数倍
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))

    // 将数据进行结构转换，方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换结构后的数据进行聚合处理
    val wordToSumDSteam: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 将结果打印出来
    wordToSumDSteam.print()


    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行
    streamingContext.awaitTermination()

  }

}
