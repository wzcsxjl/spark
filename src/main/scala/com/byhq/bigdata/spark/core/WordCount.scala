package com.byhq.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    // 使用开发工具完成Spark WordCount的开发

    // local模式
    // 创建SparkConf对象，设定Spark计算框架的运行（部署）环境，设置App名称
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    // 创建SparkContext（Spark上下文对象），该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //println(sc) //org.apache.spark.SparkContext@4d666b41

    // 使用sc创建RDD并执行相应的transformation和action

    // 读取文件，将文件内容一行一行的读取出来
    // 路径位置默认从当前的部署环境中查找
    // 如果需要从本地查找：file:///root/export/servers/spark-2.2.0-bin-2.6.0-cdh5.14.0/in
    val lines: RDD[String] = sc.textFile("in/word.txt")

    // 将一行一行的数据分解成为一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 为了统计方便，将单词数据进行结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    // 对转换后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    // 将统计结果采集后打印到控制台
    val result: Array[(String, Int)] = wordToSum.collect()
//    println(result) //[Lscala.Tuple2;@54e2fe
    result.foreach(println)

    //停止
    sc.stop()
  }
}
