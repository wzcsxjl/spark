package com.byhq.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Spark17_JSON {

  def main(args: Array[String]): Unit = {

    // 初始化Spark配置信息并建立与Spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val json: RDD[String] = sc.textFile("in/user.json")

    val result: RDD[Option[Any]] = json.map(JSON.parseFull)

    result.foreach(println)

    // 关闭与spark的连接
    sc.stop()
  }

}