package com.byhq.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_ShareData {

  def main(args: Array[String]): Unit = {

    // 初始化Spark配置信息并建立与Spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //val i: Int = dataRDD.reduce(_ + _)
    //println(i)
    var sum: Int = 0
    // 使用累加器来共享变量，来累加数据

    // 创建累加器对象
    val accumulator: LongAccumulator = sc.longAccumulator

    //dataRDD.foreach(i => sum = sum + i)
    //println("sum = " + sum)

    dataRDD.foreach {
      case i => {
        // 执行累加器的累加功能
        accumulator.add(i)
      }
    }
    println("sum = " + accumulator.value)

    // 关闭与spark的连接
    sc.stop()
  }

}