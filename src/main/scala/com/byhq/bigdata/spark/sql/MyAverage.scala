package com.byhq.bigdata.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/**
  * 定义case类
  */
case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

/**
  * 强类型UDAF函数
  */
class MyAverage extends Aggregator[Employee, Average, Double] {

  /**
    * 初始化
    * @return
    */
  override def zero: Average = Average(0L, 0L)

  /**
    * 根据传入的参数更新buffer值
    *
    * @param buffer
    * @param employee
    * @return
    */
  override def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  /**
    * 合并两个buffer值，将buffer2的值合并到buffer1
    *
    * @param buffer1
    * @param buffer2
    * @return
    */
  override def merge(buffer1: Average, buffer2: Average): Average = {
    buffer1.sum += buffer2.sum
    buffer1.count += buffer2.count
    buffer1
  }

  /**
    * 计算输出
    *
    * @param reduction
    * @return
    */
  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  /**
    * 设定中间值类型的编码器，要转换成case类
    * Encoders.product是进行scala元组和case类转换的编码器
    *
    * @return
    */
  override def bufferEncoder: Encoder[Average] = Encoders.product

  /**
    * 设定最终输出值的编码器
    *
    * @return
    */
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
