package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 需求：循环创建几个RDD，将RDD放入队列。通过SparkStream创建Dstream，计算WordCount
  */
object RDDStream {

  def main(args: Array[String]): Unit = {

    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    // 2.初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(4))

    // 3.创建RDD队列
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    // 4.创建QueueInputDStream
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    // 5.处理队列中的RDD数据
    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))
    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)

    // 6.打印结果
    reducedStream.print()

    // 7.启动任务
    ssc.start()

    // 8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }

}
