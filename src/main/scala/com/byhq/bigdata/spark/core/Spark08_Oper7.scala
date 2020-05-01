package com.byhq.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Oper7 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建Spark上下文对象
    val sc: SparkContext = new SparkContext(config)
    // 生成数据，按照指定的规则进行过滤
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)
    filterRDD.collect().foreach(println)
  }
}
