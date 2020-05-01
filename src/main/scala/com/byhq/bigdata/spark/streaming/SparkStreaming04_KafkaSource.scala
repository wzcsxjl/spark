package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从kafka中采集数据进行消费
  */
object SparkStreaming04_KafkaSource {

  def main(args: Array[String]): Unit = {

    // 使用SparkStreaming完成WordCount

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming04_KafkaSource")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 通过KafkaUtil创建kafkaDStream，从Kafka中采集数据
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop01:2181",
      "byhq",
      Map("byhq" -> 3)
    )

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
