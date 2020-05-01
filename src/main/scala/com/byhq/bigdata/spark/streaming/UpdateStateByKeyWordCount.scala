package com.byhq.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))
    streamingContext.checkpoint("wc")

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    /*val wordCount: DStream[(String, Int)] = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue: Int = state.getOrElse(0)
      for (value <- values) {
        newValue += value
      }
      Option(newValue)
    })*/

    val wordCount: DStream[(String, Int)] = pairs.updateStateByKey {
      case (values, state) =>
        val sum = state.getOrElse(0) + values.sum
        Option(sum)
    }

    wordCount.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
