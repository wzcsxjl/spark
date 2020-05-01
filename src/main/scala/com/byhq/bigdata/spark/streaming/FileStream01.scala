package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用自定义的数据源采集数据
  * 监控某个端口号，获取该端口号内容
  */
object FileStream01 {

  def main(args: Array[String]): Unit = {

    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    // 2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 3.创建自定义receiver的Streaming
    val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop01", 9999))

    // 4.将每一行数据做切分，形成一个个单词
    val wordStreams: DStream[String] = lineStream.flatMap(_.split("\t"))

    // 5.将单词映射成元组(word, 1)
    val wordAndOneStreams: DStream[(String, Int)] = wordStreams.map((_, 1))

    // 6.将相同的单词次数做统计
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    // 7.打印
    wordAndCountStreams.print()

    // 8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()

  }

}
