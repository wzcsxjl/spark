package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 需求：使用netcat工具向9999端口不断的发送数据，通过SparkStreaming读取端口数据并统计不同单词出现的次数
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {

    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 3.通过监控端口创建DStream，读进来的数据为一行一行
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01", 9999)

    // 将每一行数据做切分，形成一个个单词
    val wordStream: DStream[String] = lineStreams.flatMap(_.split(" "))

    // 将单词映射成元组(word, 1)
    val wordAndOneStreams: DStream[(String, Int)] = wordStream.map((_, 1))

    // 将相同的单词次数做统计
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    // 打印
    wordAndCountStreams.print()

    // 启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()

  }

}
