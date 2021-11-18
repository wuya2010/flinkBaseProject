package com.alibaba.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //    val inputStream = env.socketTextStream("localhost", 7777)
    val inputStream = env.readTextFile("E:\\WORKS\\Mine\\flinkBaseProject\\flinkBase\\src\\main\\resources\\sensor.txt")


    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //添加水印
//      .assignTimestampsAndWatermarks(new MyAssigner())

//    dataStream.print("orial data")

    // 1. 温度连续上升报警: 基于时间相关，温度连续上升
    val warningStream = dataStream
      .keyBy(_.id) //根据id分组
      //fixme: keyStream
      .process(new TempIncreseWarning())

    warningStream.print("process")



    // 2. 低温冰点报警：基于时间相关
    val freezingMonitorStream = dataStream
      .process(new FreezingMonitor())

    freezingMonitorStream.print("freezing")
    //输出侧输出流
    freezingMonitorStream.getSideOutput(new OutputTag[(String, String)]("freezing-warning")).print("side")


    // 3. 温度跳变报警
    val tempChangeWarningStream = dataStream
      .keyBy(_.id)
      //      .flatMap( new TempChangeWarning(10.0) )
      .flatMapWithState[(String, Double, Double, String), Double] {
      // 如果状态为空，第一条数据
      case (in: SensorReading, None) => (List.empty, Some(in.temperature))
      // 如果状态不为空，判断是否差值大于阈值
      case (in: SensorReading, lastTemp: Some[Double]) => {
        val diff = (in.temperature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((in.id, lastTemp.get, in.temperature, "change too much")), Some(in.temperature))
        } else {
          (List.empty, Some(in.temperature))
        }
      }
    }



    //    freezingMonitorStream.print("healthy")
    //    freezingMonitorStream.getSideOutput(new OutputTag[(String, String)]("freezing-warning")).print("freezing")

//    tempChangeWarningStream.print("change")

    env.execute("process function test")
  }
}

//K, I,O
class TempIncreseWarning() extends KeyedProcessFunction[String, SensorReading, String] {

  //定义状态
  // 定义一个状态，用户保存上一次的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp-state", Types.of[Double]))
  // 定义一个状态，用户保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer-state", Types.of[Long]))

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("sensor " + ctx.getCurrentKey + "温度5秒内连续上升") //主流输出
    currentTimer.clear()
  }

  //如何利用状态值？
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 取出上一次的温度值
    val prevTemp = lastTemp.value()
    lastTemp.update(value.temperature)  //赋值新的值给lastTemp

    val curTimerTs = currentTimer.value()  //当前的时间闹钟

    // 如果温度上升，而且没有设置过定时器，就注册一个定时器
    if (value.temperature > prevTemp && curTimerTs == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 1000L //温度10s连续上升, 定时器（定时器的作用）
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      // 保存时间戳到状态
      currentTimer.update(timerTs)
    }
    // 如果温度下降的话，删除定时器
    else if (value.temperature < prevTemp) {
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }
}

//实现的是： ProcessFunction
class FreezingMonitor() extends ProcessFunction[SensorReading, (String, Double, String)] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, (String, Double, String)]#Context, out: Collector[(String, Double, String)]): Unit = {
    if (value.temperature < 35.0) {
      //侧输出流； fixme: 主流与侧输出流的关系
      ctx.output(new OutputTag[(String, String)]("freezing-warning"), (value.id, "freezing")) //侧输出流
    } else {
      out.collect((value.id, value.temperature, "healthy"))
    }
  }
}

//todo:  自定义富函数
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double, String)] {
  // 定义状态，用于保存上一次的温度值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp-state", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double, String)]): Unit = {
    // 先取出上一次的温度
    val lastTemp = lastTempState.value()

    // 根据当前温度值和上次的差值，判断是否输出报警
    val diff = (lastTemp - value.temperature).abs
    if (diff > threshold) {
      out.collect((value.id, lastTemp, value.temperature, "change too much"))
    }

    // 更新状态
    lastTempState.update(value.temperature)
  }
}