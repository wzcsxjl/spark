package com.byhq.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {
    // SparkConf
    // 创建配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_Transform")
    // SparkSession
    // 创建SparkSQL的环境对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark不是包名的含义，是SparkSession对象的名字
    import spark.implicits._
    // 创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))
    // 转换为DF
    val df: DataFrame = rdd.toDF("id", "name", "age")
    // 转换为DS
    val ds: Dataset[User] = df.as[User]
    // 转换为DF
    val df1: DataFrame = ds.toDF()
    // 转换为RDD
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach(row => {
      // 获取数据时，可以通过索引访问数据
      println(row.getString(1))
    })
    // 释放资源
    spark.stop()
  }
}

/**
  * 样例类
  * @param id
  * @param name
  * @param age
  */
case class User(id: Int, name: String, age: Int)