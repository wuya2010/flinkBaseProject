package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 对数据进行窗口聚合处理
    val aggStream = inputStream
      .filter(_.behavior == "pv")
        .map( data => ("uv", data.userId) )
        .keyBy(_._1)
        .timeWindow( Time.hours(1) )
        .trigger( new MyTrigger() )
//        .process( new UvCountWithBloom() )
//      .print()

    env.execute("unique visitor with bloom job")
  }
}

// 自定义窗口触发机制，每来一条数据就触发一次窗口操作，写入到redis中
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
//    val myState: ValueState[Int] = ctx.getPartitionedState( new ValueStateDescriptor[Int]("my-state", classOf[Int]) )
//    myState.clear()
  }

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }
}

// 定义一个布隆过滤器，要求传入的size是整数
class Bloom(size: Long) extends Serializable {
  private val cap = size
  // 用hash函数实现userId到每一个位的对应关系
  def hash( value: String, seed: Int ): Long = {
//    MurmurHash3.stringHash(value)
    // 定义返回值
    var result = 0
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }
    // 返回size以内的值
    (cap - 1) & result
  }
}
//
//// 自定义process function
//class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
//  // 定义redis连接和布隆过滤器
//  lazy val jedis = new Jedis("localhost", 6379)
//  lazy val bloom = new Bloom( 1 << 28 )  // 32MB位图，存储2亿多个位
//
//  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
//    // 在redis里存储位图，以windowEnd作为key存储；另外storeKey也作为hash表中的key
//    val storeKey = context.window.getEnd.toString
//    // 把当前窗口uv的count值也存入redis，存入一张hash表，表名叫count
//    var count: Long = 0L
//    // 先获取当前的count值
//    if( jedis.hget("count", storeKey) != null ){
//      count = jedis.hget("count", storeKey).toLong
//    }
//
//    // 根据hash值，查对应偏移量的位是否有值，说明当前user是否存在
//    val userId = elements.last._2.toString
//    val offset = bloom.hash(userId, 61)
//
//    val isExist = jedis.getbit(storeKey, offset)
//    if( !isExist ){
//      // 如果不存在，就将位图对应位置置1，count + 1
//      jedis.setbit(storeKey, offset, true)
//      jedis.hset("count", storeKey, (count + 1).toString)
//      out.collect(UvCount(storeKey.toLong, count + 1))
//    }
//  }
//}