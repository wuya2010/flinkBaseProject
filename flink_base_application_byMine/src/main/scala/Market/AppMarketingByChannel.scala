package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import sun.awt.TimedWindowEvent

import scala.util.Random


// 定义源数据样例类
case class MarketingUserBehavior( userId: String, behavior: String, channel: String, timestamp: Long )
// 定义一个输出样例类
case class MarketingViewCount( windowStart: String, channel: String, behavior: String, count: Long )

// 自定义source
class SimulatedEventSource extends RichSourceFunction[MarketingUserBehavior]{
  // 表示是否运行的标识位
  var running = true
  // 定义渠道的集合
  val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "XiaomiStore", "weibo", "wechat")
  // 定义用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK","DOWNLOAD","UPDATE","INSTALL","UNINSTALL")
  // 定义一个随机数生成器
  val rand: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个最大生成数据的量，和当前偏移量
    val maxCount = Long.MaxValue
    var count = 0L

    while( running && count < maxCount ){
      // 随机生成所有字段
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes( rand.nextInt(behaviorTypes.size) )
      val channel = channelSet( rand.nextInt(channelSet.size) )
      val ts = System.currentTimeMillis()

      ctx.collect( MarketingUserBehavior(id, behavior, channel, ts) )

      count += 1
      Thread.sleep(10L)
    }
  }
}

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.addSource( new SimulatedEventSource )
      .assignAscendingTimestamps(_.timestamp)

    val dataStream = inputStream
      .filter(_.behavior != "UNINSTALL")
      .map( data => ( (data.channel, data.behavior), 1L ) )
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process( new MarketingCountByChannel() )
      .print()

    env.execute("app marketing by channel job")
  }
}

class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp( context.window.getStart )
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect( MarketingViewCount(startTs.toString, channel, behavior, count) )
  }
}