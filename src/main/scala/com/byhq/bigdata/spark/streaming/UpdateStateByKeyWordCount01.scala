package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyWordCount01 {

  def main(args: Array[String]): Unit = {
    // 定义状态更新方法，参数valus为当前批次单词频度，state为以往单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.foldLeft(0)(_ + _)
      val previousCount: Int = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

      val sc: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(sc, Seconds(3))
      ssc.checkpoint("streamCheck")

      // Create a DStream that will connect to hostname:port, like localhost:9999
      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

      // Split each line into words
      val words: DStream[String] = lines.flatMap(_.split(" "))

      // Count each word in each batch
      val pairs: DStream[(String, Int)] = words.map((_, 1))

      // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
      val stateDStream: DStream[(String, Int)] = pairs.updateStateByKey[Int](updateFunc)
      stateDStream.print()

      ssc.start()
      ssc.awaitTermination()
  }

}
