package com.byhq.bigdata.spark.core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

// 自定义累加器
object Spark21_Accumulator {

  def main(args: Array[String]): Unit = {

    // 初始化Spark配置信息并建立与Spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[String] = sc.makeRDD(List("hadoop", "hive", "hbase", "scala", "spark"), 2)

    //val i: Int = dataRDD.reduce(_ + _)
    //println(i)
    //var sum: Int = 0
    // 使用累加器来共享变量，来累加数据

    // 创建累加器对象
    //val accumulator: LongAccumulator = sc.longAccumulator

    //dataRDD.foreach(i => sum = sum + i)
    //println("sum = " + sum)

    // TODO 创建累加器
    val wordAccumulator = new WordAccumulator
    // TODO 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach {
      case word => {
        // TODO 执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }
    // TODO 获取累加器的值
    println("sum = " + wordAccumulator.value)

    // 关闭与spark的连接
    sc.stop()
  }

}

// 声明累加器
// 1. 继承AccumulatorV2
// 2. 实现抽象方法
// 3. 创建累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list: util.ArrayList[String] = new util.ArrayList[String]()

  // 当前的累加器是否为初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  // 重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}