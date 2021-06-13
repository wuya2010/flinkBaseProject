package flink_process

import flink_source.SensorReading
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.omg.CORBA.Current

/**
  * @author kylinWang
  * @data 2020/7/12 11:03
  *
  */
object processTest {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val inputStream = env.socketTextStream("localhost", 4444)
    //对输入的数据进行解析
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })



    // 连续上升报警 基于时间相关，温度连续上升
    val warningSteam = dataStream
      .keyBy(_.id)
      //Applies the given [[KeyedProcessFunction]] on the input stream
      //根据keyedProcessing ==> creating a transformed output stream
      //直接用process调用底层的API
      .process(new IncreseWarning())

    //冰点报警 : 状态基于时间
     val freezingStream = dataStream
       .process(new FreezingWarning())

    //温度跳变: 富函数实现
    val  tempChangeWarningStream = dataStream
      //todo: 有兴趣可以了解一下
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


    warningSteam.print("increseing")

    freezingStream.print("warning")

    tempChangeWarningStream.print("flatmap")

    env.execute("process flink")
  }
}

//温度上升: KeyedProcessFunction用来操作KeyedStream   ----基于一个key： 所以是key ， in，  out
//思路：每来一条数据就会调用一次该方法
// 1. onTimer 方法为回调函数，当定时器到达该时间时就会触发，它根据时间的推进自行触发，不需额外设定，且使用完毕后需要清空状态
// 2. process 方法，来一条数据就会调用一次，该方法需要保存状态（值状态，时间状态），以便进行比较，设置定时器规则（比如： 什么情况下设置定时器）
class IncreseWarning() extends KeyedProcessFunction[String, SensorReading, String]{

  //定义状态：保存温度值
  lazy val lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("",Types.of[Double]))
  //定义另一个状态，保存定时器的时间戳  因为删除定时器需要一个时间戳
  lazy val currentTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer-state", Types.of[Long]))

  //在记录了状态后，需要一个定时器触发的逻辑 ； 当注册的定时器触发时调用， 相当于一个闹钟--取时间周期的数据
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String])={

     out.collect("sensor " + ctx.getCurrentKey + "温度10秒内连续上升")//谁连续上升？
     currentTimer.clear()  //定时器： 清空定时器
  }

  //每个元素执行的逻辑，来一个就进行对比，如果在这个过程中，上升，保存数据，达到10s，就报警
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    val prevTemp: Double = lastTemp.value()
    val nowTemp: Double = value.temperature
    //更新状态: 把value状态中的值给lastTemp, 也可在代码最后进行状态更新
    lastTemp.update(nowTemp)

    //当前的定时器
    val curTimerTs = currentTimer.value()
    if(nowTemp > prevTemp && curTimerTs == 0 ){
      //开始注册一个闹钟
      val TimeTs = ctx.timerService().currentProcessingTime() + 10* 1000L
      ctx.timerService().registerProcessingTimeTimer(TimeTs)
      //更新时间表
      currentTimer.update(TimeTs)
    }else{
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs) //删除当前的定时器
      currentTimer.clear()
    }
  }
}


  //冰点 ：输出到侧输出流
  class FreezingWarning()  extends  ProcessFunction[SensorReading, (String, Double, String)]{
    //todo： 这里看一下视频
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, (String, Double, String)]#Context, out: Collector[(String, Double, String)]): Unit = {
        val nowTem = value.temperature
        if(nowTem < 32.0){
          // outputTag : （名称， value）
            ctx.output( new OutputTag[(String,String)]("freezing-warning"), (value.id, "freezing"))
        }else{
            out.collect(value.id,value.temperature, "healthy")
        }
    }
}


