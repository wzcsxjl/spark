package com.byhq.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区器
  */
object Spark13_Oper12 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建Spark上下文对象
    val sc: SparkContext = new SparkContext(config)
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val partitionRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))
    partitionRDD.saveAsTextFile("output")
  }
}

/**
  * 声明分区器
  * 继承Partitioner类
  *
  * @param partitions
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}
