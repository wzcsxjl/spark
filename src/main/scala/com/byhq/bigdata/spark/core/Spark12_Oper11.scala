package com.byhq.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Oper11 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建Spark上下文对象
    val sc: SparkContext = new SparkContext(config)
    /*val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("重新分区前=" + listRDD.partitions.size)
    val repartitionRDD: RDD[Int] = listRDD.repartition(3)
    // 根据分区数，重新通过网络随机洗牌所有数据
    // coalesce(numPartitions, shuffle = true) repartition底层调用coalesce，有shuffle过程
    println("重新分区后=" + repartitionRDD.partitions.size)
    repartitionRDD.saveAsTextFile("output")*/

    val listRDD = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    listRDD.saveAsTextFile("output1")
    listRDD.saveAsSequenceFile("output2")
    listRDD.saveAsObjectFile("output3")
  }
}
