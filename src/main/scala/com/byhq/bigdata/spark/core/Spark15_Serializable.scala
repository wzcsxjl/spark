package com.byhq.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_Serializable {

  def main(args: Array[String]): Unit = {

    // 初始化Spark配置信息并建立与Spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "boya"))

    val search: Search = new Search("h")

    //val match1: RDD[String] = search.getMatch1(rdd)
    val match1: RDD[String] = search.getMatche2(rdd)
    match1.collect().foreach(println)

    // 关闭与spark的连接
    sc.stop()
  }

}


//class Search(query: String) extends java.io.Serializable {
class Search(query: String) {

  // 过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)
  }

  // 过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  // 过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
    val q = query
    rdd.filter(x => x.contains(q))
  }

}