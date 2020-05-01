package com.byhq.bigdata.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置名称
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._

    val dataFrame: DataFrame = sparkSession.read.json("in/user.json")

    // Displays the content of the DataFrame to stdout
    dataFrame.show()

    dataFrame.filter($"age" > 21).show()

    dataFrame.createOrReplaceTempView("user")

    sparkSession.sql("select * from user where age > 21").show()

    sparkSession.stop()
  }

}
