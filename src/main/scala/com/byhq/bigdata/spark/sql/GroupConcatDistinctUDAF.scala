package com.byhq.bigdata.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 用户自定义聚合函数
  * 弱类型UDAF
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

  /**
    * 聚合函数输入参数的数据类型
    * @return
    */
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  /**
    * 聚合缓冲区中值的类型
    * 中间进行聚合时所处理的数据类型
    *
    * @return
    */
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  /**
    * 函数返回值的数据类型
    * @return
    */
  override def dataType: DataType = StringType

  /**
    * 一致性检验，如果为true，那么输入不变的情况下计算的结果也是不变的
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 设置聚合中间buffer的初始值
    * 需要保证这个语义：两个初始buffer调用下面实现的merge方法后也应该为初始buffer，
    * 即如果初始值是1，然后merge是执行一个相加的动作，两个初始buffer合并之后等于2，不会等于初始buffer了。
    * 这样的初始值就是有问题的，所以初始值也叫“zero value”
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  /**
    * 用输入数据input更新buffer值。类似于combineByKey
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 缓冲中的已经拼接过的城市信息
    var bufferCityInfo: String = buffer.getString(0)
    // 刚刚传递进来的某个城市信息
    val cityInfo: String = input.getString(0)
    // 在这里要实现去重的逻辑
    // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
    if (!bufferCityInfo.contains(cityInfo)) {
      if ("".equals(bufferCityInfo))
        bufferCityInfo += cityInfo
      else {
        // 如1:北京
        // 1:北京,2:上海
        bufferCityInfo += "," + cityInfo
      }

      buffer.update(0, bufferCityInfo)
    }
  }

  /**
    * 合并两个buffer，将buffer2合并到buffer1，在合并两个分区聚合结果的时候会被用到，类似于reduceByKey
    * 这里要注意该方法没有返回值，在实现的时候是把buffer2合并到buffer1中去，你需要实现这个合并细节
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    val bufferCityInfo2 = buffer2.getString(0)

    for (cityInfo <- bufferCityInfo2.split(",")) {
      if (!bufferCityInfo1.contains(cityInfo)) {
        if ("".equals(bufferCityInfo1)) {
          bufferCityInfo1 += cityInfo
        }
        else {
          bufferCityInfo1 += "," + cityInfo
        }
      }
    }

    buffer1.update(0, bufferCityInfo1)
  }

  /**
    * 计算并返回最终的聚合结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }

}
