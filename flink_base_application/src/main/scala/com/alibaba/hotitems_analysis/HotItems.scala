package com.alibaba.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.hotitems_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/24 14:23
  */

// 输入用户行为数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

  /*  val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")*/

    // 读取数据
    // val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
    val inputStream = env.socketTextStream("hadoop102",7777)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 对数据进行窗口聚合处理
    val aggStream: DataStream[ItemViewCount] = inputStream
      .filter(_.behavior == "pv") // 过滤出pv数据
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5)) // 开窗进行统计
      .aggregate(new CountAgg(), new WindowCountResult()) // 聚合出当前商品在时间窗口内的统计数量

    // 对聚合结果按照窗口分组，并排序
    val processedStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3)) // 用process function做排序处理，得到top N

//    inputStream.print("input")
//    aggStream.print("agg")
    processedStream.print("process")

    env.execute("Hot items job")
  }
}

// 自定义的预聚合函数，来一条数据就加一
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 示例：求取平均数
class MyAverageAgg() extends AggregateFunction[Long, (Long, Int), Double] {
  override def add(value: Long, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value, accumulator._2 + 1)

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2.toDouble

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

// 自定义window function
class WindowCountResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  //  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //  val itemId = key.asInstanceOf[Tuple1[Long]].f0
  //    val windowEnd = window.getEnd
  //    val count = input.iterator.next()
  //    out.collect( ItemViewCount(itemId, windowEnd, count) )
  //  }
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义process function
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 定义一个列表状态，用于保存所有的商品个数统计值
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemList-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，就保存入list state，注册一个定时器
    itemListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 先将所有数据从状态中取出
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemListState.get()) {
      allItems += item
    }
    itemListState.clear()

    // 按照点击量从大到小排序，并取Top N
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将信息格式化为String，方便打印输出
    val results: StringBuilder = new StringBuilder()
    results.append("时间：").append(new Timestamp(timestamp)).append("\n")
    // 对排序的数据遍历输出
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      results.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 点击量=").append(currentItem.count)
        .append("\n")
    }
    results.append("=====================================")
    Thread.sleep(1000L)
    out.collect(results.toString())
  }
}