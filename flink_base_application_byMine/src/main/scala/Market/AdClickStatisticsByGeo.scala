

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


// 输入数据样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
// 输出按照省份划分的点击统计结果
case class AdCountByProvince( windowEnd: String, province: String, count: Long )
// 侧输出流的黑名单报警信息样例类
case class BlackListWarning( userId: Long, adId: Long, msg: String )

object AdClickStatisticsByGeo {
  val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源
    val resource = getClass.getResource("/AdClickLog.csv")
    val adClickEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        //数据格式： 543462,1715,beijing,beijing,1511658000
        AdClickEvent( dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)//周期性生成时间戳

    // 添加黑名单过滤的逻辑
    val filterBlackListStream = adClickEventStream
      .keyBy( data => (data.userId, data.adId) )
      .process( new FilterBlackListUser(100) )

    // 进行开窗聚合统计
    val adCountStream = filterBlackListStream
      .keyBy(_.province)     // 按照省份分组
      .timeWindow( Time.hours(1), Time.seconds(10) )
      .aggregate( new CountAgg(), new AdCountResult() ) //窗口聚合,没有进一步的处理

    adCountStream.print("count")
    filterBlackListStream.getSideOutput( blackListOutputTag ).print("black list")//测输出流：多运行，多跑

    env.execute("ad count job")
  }



  // 实现自定义的process function
  // 实现： 过滤黑名单
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
    // 定义状态，保存用户对广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 标识位，标记是否发送过黑名单信息
    lazy val isSent: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))
    // 保存定时器触发的时间戳
    lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      // 获取当前的count值
      val curCount = countState.value()
      // 判断如果是第一条数据，count值是0，就注册一个定时器
      if( curCount == 0 ){
        val ts = ( ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1 ) * 24 * 60 * 60 * 1000L
        ctx.timerService().registerProcessingTimeTimer(ts)
        resetTime.update(ts)
      }
      countState.update( curCount + 1 )
      // 判断计数是否超出上限，如果超过输出黑名单信息到侧输出流
      if( curCount >= maxCount ){
        // 判断如果没有发送过黑名单信息，就输出
        if( !isSent.value() ){
          ctx.output( blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today") )
          isSent.update(true)
        }
      } else {
        out.collect( value )
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 如果当前定时器是重置状态定时器，那么清空状态
      if( timestamp == resetTime.value() ){
        isSent.clear()
        countState.clear()
      }
    }
  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数
class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    val windowEnd = new Timestamp(window.getEnd).toString
    out.collect( AdCountByProvince( windowEnd, key, input.iterator.next() ) )
  }
}