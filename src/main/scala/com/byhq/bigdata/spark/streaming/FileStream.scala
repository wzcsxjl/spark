package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 文件数据源
  */
object FileStream {

  def main(args: Array[String]): Unit = {

    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FileStream")

    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 3.监控文件夹创建DStream
    val dirStream: DStream[String] = ssc.textFileStream("hdfs://hadoop01:8020/fileStream")

    // 4.将每一行数据做切分，形成一个个单词
    val wordStream: DStream[String] = dirStream.flatMap(_.split("\t"))

    // 5.将单词映射成元组(word, 1)
    val wordAndOneStreams: DStream[(String, Int)] = wordStream.map((_, 1))

    // 6.将相同的单词次数做统计
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_ + _)

    // 7.打印
    wordAndCountStreams.print()

    // 8.启动SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()

  }

}
