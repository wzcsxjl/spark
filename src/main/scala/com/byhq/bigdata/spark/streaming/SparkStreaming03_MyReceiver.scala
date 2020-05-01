package com.byhq.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 自定义采集器
  */
object SparkStreaming03_MyReceiver {

  def main(args: Array[String]): Unit = {

    // 使用SparkStreaming完成WordCount

    // Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming03_MyReceiver")

    // 实时数据分析环境对象
    // 采集周期：以指定的时间为周期采集实时数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 从指定端口号采集数据
    val receiverDStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("localhost", 9999))

    // 将采集的数据进行分解（扁平化）
    val wordDStream: DStream[String] = receiverDStream.flatMap(line => line.split(" "))

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

/**
  * 声明采集器
  * 1) 继承Receiver
  * 2) 重写方法 onStart onStop
  */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var socket: Socket = _

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()
  }

  def receive(): Unit = {
    socket = new Socket(host, port)

    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    var line: String = null

    while ((line = reader.readLine()) != null) {
      // 将采集的数据存储到采集器的内部进行转换
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }
  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}