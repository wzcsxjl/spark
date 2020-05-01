package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 转换
  */
object SparkStreaming07_Transform {

  def main(args: Array[String]): Unit = {


    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_Transform")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))



    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行
    streamingContext.awaitTermination()

  }

}
