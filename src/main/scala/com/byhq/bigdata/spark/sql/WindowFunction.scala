package com.byhq.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object WindowFunction {

  case class Score(name: String, cla: Int, score: Int)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("score").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    val scoreDF: DataFrame = sparkSession.sparkContext.makeRDD(Array(Score("a1", 1, 80),
      Score("a2", 1, 78),
      Score("a3", 1, 95),
      Score("a4", 2, 74),
      Score("a5", 2, 92),
      Score("a6", 3, 99),
      Score("a7", 3, 99),
      Score("a8", 3, 45),
      Score("a9", 3, 55),
      Score("a10", 3, 78))).toDF("name", "class", "score")
    scoreDF.createOrReplaceTempView("score")
    //scoreDF.show()

    // 1. 聚合开窗函数

    /*开窗函数count(*) over()对于查询结果的每一行都返回所有符合条件的行的条数。
    如果over关键字后的括号中的选项为空，则开窗函数会对结果集中的所有行进行聚合运算。*/
    //sparkSession.sql("select name, class, score, count(name) over() name_count from score").show()

    /*
    over关键字后的括号中可以添加选项用以改变进行聚合运算的窗口范围。
    开窗函数的over关键字后括号中可以使用partition by子句来定义行的分区来进行聚合计算。
    与group by子句不同，partition by子句创建的分区是独立于结果集的，
    创建的分区只是用来进行聚合计算的，不同的开窗函数所创建的分区也不会相互影响。
    以下SQL语句用于显示按照班级分组后每组的人数。
    over(partition by class)表示对结果按照class进行分区，并且计算当前行所属的组的聚合计算结果。
     */
    //sparkSession.sql("select name, class, score, count(name) over(partition by class) name_count from score").show()

    /*在同一个select语句中可以同时使用多个开窗函数，这些开窗函数不会相互干扰。
    下面的SQL语句用于显示每位学生的信息、同班级人数和相同分数人数*/
    /*sparkSession.sql("select name, class, score, " +
      "count(name) over(partition by class) name_class_count, " +
      "count(name) over(partition by score) name_score_count from score").show()*/


    // 2. 排序开窗函数
    // 排序开窗函数支持的开窗函数分别为：row_number(行号)、rank(排名)、dense_rank(密集排名)和ntile(分组排名)

    // row_number() over(order by score) as rownum，这个排序开窗函数是按score升序的方式来排序并得出排序结果的序号。
    //sparkSession.sql("select name, class, score, row_number() over(order by score) rownum from score").show()
    // row_number() over(order by score desc) rownum，按score降序排序
    //sparkSession.sql("select name, class, score, row_number() over(order by score desc) rownum from score").show()

    /*rank() over(order by score) as rank这个排序开窗函数是按score升序排序并得出排序结果的排名号。
    这个函数求出的排名结果可以并列，并列排名之后的排名是并列的排名加上并列数（如1, 1, 1, 4, ...）*/
    //sparkSession.sql("select name, class, score, rank() over(order by score) rank from score").show()

    /*dense_rank() over(order by score) as dense_rank这个函数与rank()函数的不同在于，
    并列排名之后的排名是并列排名加1（如1, 1, 1, 2, ...）*/
    //sparkSession.sql("select name, class, score, dense_rank() over(order by score) dense_rank from score").show()

    // ntile(6) over(order by score) ntile是按score升序排序，然后6等分分成6个组并显示所在组的序号
    //sparkSession.sql("select name, class, score, ntile(6) over(order by score) ntile from score").show()

    // 排序函数和聚合开窗函数类似，也可以在over子句中使用partition by语句，不过partition by子句要放置在order by子句之前
    sparkSession.sql("select name, class, score, " +
      "row_number() over(partition by class order by score) rownum from score").show()
  }

}