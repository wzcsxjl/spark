package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 有状态转化操作
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {

    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.foldLeft(0)(_ + _)
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("hdfs://hadoop01:8020/streamCheck")

    // Create a DStream that will connect to hostname:port
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop01", 9999)

    // Split each line into words
    val words: DStream[String] = lines.flatMap(_.split(" "))

    // import org.apache.spark.streaming.StreamingContext._ (not necessary since Spark 1.3)
    // Count each word in each batch
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDStream: DStream[(String, Int)] = pairs.updateStateByKey[Int](updateFunc)
    stateDStream.print()

    //val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)


    // Print the first ten elements of each RDD generated in this DStream to the console
    //wordCounts.print()

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

    //ssc.stop()
  }
}
