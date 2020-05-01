package com.byhq.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    // SparkConf
    // 创建配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL02_Transform")
    // SparkSession
    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 读取数据，构建DataFrame
    val frame: DataFrame = spark.read.json("in/user.json")
    // 将DataFrame转换为一张表
    frame.createOrReplaceTempView("user")
    // 采用sql的语法访问数据
    spark.sql("select * from user").show
    // 释放资源
    spark.stop()
  }
}
