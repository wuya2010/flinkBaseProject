package com.alibaba.apitest

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 *
 *
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest
 * Version: 1.0
 *
 * 2019/10/21 10:19
 */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getConfig.setAutoWatermarkInterval(300L)
    env.enableCheckpointing(60000L)

    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    env.getCheckpointConfig.setCheckpointTimeout(90000L)
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000L)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //    env.setStateBackend(new RocksDBStateBackend(""))

//        val inputStream = env.readTextFile("E:\\WORKS\\Mine\\flinkBaseProject\\flinkBase\\src\\main\\resources\\sensor.txt")
    val inputStream = env.socketTextStream("192.168.25.229", 7777)


    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //fixme: 生成 watermark 的 3 种方法: watermark 不在流中直接存在，而是在 content 中
      //.assignAscendingTimestamps(_.timestamp * 1000L)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(2500)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })
    // 自定义一个 watermark
    //      .assignTimestampsAndWatermarks( new MyAssigner() )


    val outputTag = new OutputTag[SensorReading]("side") //侧输出流

    // 每个传感器每隔10秒输出这段时间内的最小值
    val minTempPerWindowStream = dataStream
      .keyBy(_.id)
      //      .timeWindow(Time.seconds(15))  //fixme: 与 window 的区别是什么?
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10), Time.hours(-8)))
      .allowedLateness(Time.seconds(2)) //允许延迟
      .sideOutputLateData(outputTag) //侧输出
      .minBy("temperature")

    /**
     * 1. 根据 key 值进行输出， 不同的 key 分别输出
     * 2. 当达到窗口的关闭时间时，分别输出不同key的最小值
     *
     * sensor_2, 1547718199, 36.8
     * sensor_2, 1547718200, 25.0
     * sensor_2, 1547718201, 25.0
     * sensor_2, 1547718202, 25.0
     * sensor_2, 1547718203, 25.0
     * ==> 此时窗口关闭， min> SensorReading(sensor_2,1547718199,36.8)
     *
     * sensor_2, 1547718200, 25.0
     * sensor_2, 1547718201, 25.0
     * sensor_2, 1547718202, 25.0
     * sensor_2, 1547718203, 25.0
     * sensor_2, 1547718211, 16.0
     * sensor_2, 1547718212, 16.0
     * sensor_2, 1547718213, 16.0
     *
     * 200-210的窗口关闭， 获取最小值： min> SensorReading(sensor_2,1547718200,25.0)
     *
     * ==》 验证延迟时间
     * sensor_2, 1547718198, 25.2
     * sensor_2, 1547718199, 20.2
     * sensor_2, 1547718203, 16.0  ==》 有结果输出，窗口没有关闭 min> SensorReading(sensor_2,1547718199,20.2)
     * sensor_2, 1547718199, 18.2  ==> 更新最小值，  min> SensorReading(sensor_2,1547718199,18.2)
     * sensor_2, 1547718205, 16.0  ==》 2.5+2s的延迟， 此时窗口彻底关闭
     * sensor_2, 1547718199, 10.2  ==》 无法更新min最小值 ， 窗口【190-200）
     *
     *
     * 3. fixme: 没有配置 allowedLateness ， 则窗口在watermark 达到后，真正关闭，迟到数据无法进入进入窗口
     */


    //      .reduce( (x, y) => SensorReading( x.id, y.timestamp, x.temperature.min(y.temperature) ) )


    //    获得3种种的方式
    //    dataStream.print("data")
    minTempPerWindowStream.print("min")
    minTempPerWindowStream.getSideOutput(outputTag).print("side")

    env.execute("window test")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  // 定义一个最大延迟时间
  val bound: Long = 1000L
  // 定义当前最大的时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000L
  }
}

class MyAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1") {
      new Watermark(extractedTimestamp)
    } else {
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}