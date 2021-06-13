package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer



// 输入数据样例类
case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url: String )
// 聚合结果样例类
case class UrlViewCount( url: String, windowEnd: Long, count: Long )

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

//    val dataStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val inputStream = env.socketTextStream("hadoop102", 7777)
      .map( data => {
        val dataArray = data.split(" ")
        // 转换时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse( dataArray(3).trim ).getTime
        ApacheLogEvent( dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      } )//不同的时间的处理

    val aggStream = inputStream
      .filter(_.method == "GET")
      .filter( data => {
        val pattern = "^((?!\\.(css|js|png|ico)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      } )
      .keyBy(_.url)
      .timeWindow( Time.minutes(10), Time.seconds(5) )
      .allowedLateness(Time.minutes(1))    // 允许迟到数据叠加到窗口聚合结果上
      .aggregate( new CountAgg(), new UrlCountResult() )

    val processedStream = aggStream
      .keyBy(_.windowEnd)
      .process( new TopNHotUrls(5) )

    inputStream.print("input")
    aggStream.print("agg")
    processedStream.print("process")

    env.execute("Network flow job")
  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
// 自定义窗口函数
class UrlCountResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect( UrlViewCount( key, window.getEnd, input.iterator.next() ) )
  }
}

// 自定义process function实现排序输出
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
  lazy val urlMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("urlMap-state", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
}

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViewCounts: ListBuffer[(String, Long)] = ListBuffer()
//    for( urlViewCount <- urlListState.get() ){
//      allUrlViewCounts += urlViewCount
//    }
    val iter = urlMapState.entries().iterator()
    while( iter.hasNext ){
      val entry = iter.next()
      allUrlViewCounts += ( ( entry.getKey, entry.getValue ) )
    }
//    urlMapState.clear()
    // 排序
    val sortedUrlViewCounts = allUrlViewCounts.sortWith( _._2 > _._2 ).take(topSize)

    // 格式化输出
    val results: StringBuilder = new StringBuilder()
    results.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    // 对排序的数据遍历输出
    for (i <- sortedUrlViewCounts.indices) {
      val currentItem = sortedUrlViewCounts(i)
      results.append("No").append(i + 1).append(":")
        .append(" URL=").append(currentItem._1)
        .append(" 点击量=").append(currentItem._2)
        .append("\n")
    }
    results.append("=====================================")
    Thread.sleep(1000L)
    out.collect(results.toString())
  }
}