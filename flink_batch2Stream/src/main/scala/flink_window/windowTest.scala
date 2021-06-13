package flink_window

import flink_source.SensorReading
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @author kylinWang
  * @data 2020/7/12 11:03
  *
  */
object windowTest {
  def main(args: Array[String]): Unit = {
    //增加注释
     val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //todo: 本地测试： nc -l -p  4444
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 4444)
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    dataStream.print("get_ds")

    //方式一： 对于默认排好序列的,事先得知数据流的时间戳是单调递增的，也就是说没有乱序
    //直接使用数据的时间戳生成watermark，把时间戳的字段传入进去
    //这里的参数参入的是毫秒数
    val watermark_1 = dataStream.assignAscendingTimestamps(_.timestamp * 1000L)

    //方式二：乱序数据流
    // todo: 了解assignTimestampsAndWatermarks
    //这个是一个比较普遍的方法
    //参数： assigner: AssignerWithPeriodicWatermarks[T]==》分配器  周期性生成
    //BoundedOut   implements   AssignerWithPeriodicWatermarks  参数：  maxOutOfOrderness：最大乱序时间
    val watermark_2 = dataStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000L)) { //todo: 这里需要传入的参数--  maxOutOfOrderness: Time
          //每次传入一条数据，提取时间戳
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      }
    )

    //方式三：自定义: 分为周期性生成watermark 和 间断性 生成 watermark
    val watermark_3 = dataStream.assignTimestampsAndWatermarks(new MyAssigner1())
    val watermark_4 = dataStream.assignTimestampsAndWatermarks(new MyAssigner2())


    //todo: 侧输出流的使用的标签
    val outputTag = new OutputTag[SensorReading]("side")

    //时间周期内, 取最小的值
    //滑动窗口
    val slid_window: SlidingEventTimeWindows = SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(10))

    val minTemperature = watermark_3
      .keyBy(_.id)
      .window(slid_window)
      .allowedLateness(Time.seconds(10)) //todo: 允许延迟只对 event-time windows 有效， 允许迟到的时间
      //测输出流：Send late arriving data to the side output identified by the given [[OutputTag]]
      .sideOutputLateData(outputTag)
      .minBy("temperature") //窗口内的聚合函数


    //todo: 实际测试一把
    dataStream.print("源数据")
    minTemperature.print("window流")
    minTemperature.getSideOutput(outputTag).print("测输出流")


    env.execute("flink window")
  }
}








//周期性生成: AssignerWithPeriodicWatermarks
class MyAssigner1() extends AssignerWithPeriodicWatermarks[SensorReading]{

  // 定义一个最大延迟时间
  val bound: Long = 1000L
  // 定义当前最大的时间戳
  var maxTs: Long = Long.MinValue

  //获取当前的水印
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  //提取时间戳
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000L) //比较大小
    element.timestamp * 1000L  //当前时间作为时间戳
  }
}


//间断性生成：
class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp) //根据当前时间戳生成watermark
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}