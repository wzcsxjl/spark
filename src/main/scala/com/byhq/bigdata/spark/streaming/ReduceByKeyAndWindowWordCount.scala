package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReduceByKeyAndWindowWordCount {

  def main(args: Array[String]): Unit = {
    // 定义更新状态方法，参数values为当前批次单词频度，state为以往单词频度
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.foldLeft(0)(_ + _)
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val sc: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(3))
    ssc.checkpoint("./501")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words: DStream[String] = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    val wordCounts: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b),
      Seconds(12),
      Seconds(6))

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }

}
