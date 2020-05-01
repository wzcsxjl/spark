package com.byhq.bigdata.spark.core

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_MySQL {

  def main(args: Array[String]): Unit = {

    // 初始化Spark配置信息并建立与Spark的连接
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    // 定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "123456"

    // 创建JdbcRDD，访问数据库
    // 查询数据
    /*
    val sql: String = "select name, age from user where id >= ? and id <= ?"
    val jdbcRDD: JdbcRDD[Unit] = new JdbcRDD(
      sc,
      () => {
        // 获取数据库连接对象
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      sql,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString(1) + "，" + rs.getInt(2))
      }
    )
    jdbcRDD.collect()
    */
    // 保存数据
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangwu", 40)), 2)
    /*
    dataRDD.foreach {
      // 模式匹配
      case (username, age) => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql: String = "insert into user (name, age) values (?, ?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)
        statement.setString(1, username)
        statement.setInt(2, age)
        statement.executeUpdate()

        statement.close()
        connection.close()
      }
    }
    */

    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      datas.foreach {
        case (username, age) => {
          val sql: String = "insert into user (name, age) values (?, ?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()

          statement.close()
        }
      }
      connection.close()
    })


      // 关闭与spark的连接，释放资源
      sc.stop()
    }

  }