package com.byhq.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkRDDtoDF {

  // 1. 通过编程获取Schema
  def rddToDF(sparkSession: SparkSession): DataFrame = {
    // 设置schema结构
    val schema: StructType = StructType(
      Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    val rowRDD: RDD[Row] = sparkSession.sparkContext
      .textFile("D:\\spark\\person.txt", 2)
      .map(x => x.split(" "))
      .map(x => Row(x(0), x(1).trim().toInt))
    sparkSession.createDataFrame(rowRDD, schema)
  }

  // 2. 通过反射方式获取Schema
  case class Person(name: String, age: Int)
  def rddToDFCase(sparkSession: SparkSession): DataFrame = {
    // 导入隐式操作，否则RDD无法调用toDF方法
    import sparkSession.implicits._
    val personDF: DataFrame = sparkSession.sparkContext
      .textFile("D:\\spark\\person.txt", 2)
      .map(x => x.split(" "))
      .map(x => Person(x(0), x(1).trim().toInt)).toDF()
    personDF
  }

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]")
    sparkConf.set("spark.sql.warehouse.dir", "D:\\spark\\")
    sparkConf.set("spark.sql.shuffle.partitions", "20")
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("RDD to DataFrame")
      .config(sparkConf)
      .getOrCreate()
    // 通过代码的方式，设置Spark log4j的级别
    sparkSession.sparkContext.setLogLevel("WARN")
    import sparkSession.implicits._
    // use case class convert RDD to DataFrame
    // val personDF: DataFrame = rddToDFCase(sparkSession)

    // 通过编程方式将RDD转换为DataFram
    val peopleDF = rddToDF(sparkSession)

    // peopleDF.show()
    peopleDF.select($"name", $"age").filter($"age" > 25).show()
  }

}
