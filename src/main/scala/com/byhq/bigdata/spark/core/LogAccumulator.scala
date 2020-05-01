package com.byhq.bigdata.spark.core

import java.util
import java.util.Collections

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * 自定义累加器
  */
// AccumulatorV2[IN, OUT]
class LogAccumulator extends AccumulatorV2[String, util.Set[String]] {

  // 保存所有聚合数据
  private val _logArray: util.HashSet[String] = new util.HashSet[String]()

  // 判断是否为初始值
  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc: LogAccumulator = new LogAccumulator
    _logArray.synchronized {
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  // 重置累加器中的值
  override def reset(): Unit = {
    _logArray.clear()
  }

  // 向累加器中添加另一个值
  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  // 各个task的累加器进行合并的方法
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }
  }

  // 获取累加器中的值
  override def value: util.Set[String] = {
    Collections.unmodifiableSet(_logArray)
  }
}

/**
  * 过滤掉带字母的
  */
object LogAccumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LogAccumulator").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val accum: LogAccumulator = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum: Int = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern: String = """^_?(\d+)"""
      val flag: Boolean = line.matches(pattern)
      // 过滤掉带字母的元素
      if (!flag) {
        // 将不含字母的元素添加到新的数组中
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)
    println("sum: " + sum)
    // 取出的值是带字母的
    for (v <- accum.value) print(v + " ")
    sc.stop()
  }
}
