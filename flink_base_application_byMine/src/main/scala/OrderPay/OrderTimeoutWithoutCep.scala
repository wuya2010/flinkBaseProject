package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object OrderTimeoutWithoutCep {
  // 定义一个侧输出流tag
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源，包装成样例类
    val resource = getClass.getResource("/OrderLog.csv")
//    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 7777)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    val orderResultStream = orderEventStream
      .process( new OrderPayMatch() )

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout without cep job")
  }

  // 自定义实现匹配和超时的处理函数
  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
    // 定义一个标记位，用于判断pay事件是否来过
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean]))
    // 定义一个状态，用来保存定时器时间戳
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      // 先拿到状态
      val isPayed = isPayedState.value()
      val timerTS = timerState.value()

      // 根据来的事件类型分别处理
      if( value.eventType == "create" ){
        // 判断是否已经pay过
        if( isPayed ){
          out.collect( OrderResult(value.orderId, "payed successfully") )
          // 清空状态和定时器
          ctx.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 注册定时器
          val ts = value.eventTime * 1000L + 900 * 1000L
          ctx.timerService().registerEventTimeTimer( ts )
          timerState.update(ts)
        }
      } else if( value.eventType == "pay" ){
        if( timerTS > 0 ){
          // 如果定时器有值，说明create来过
          if( value.eventTime * 1000L < timerTS ){
            // 如果小于定时器时间，正常匹配
            out.collect( OrderResult(value.orderId, "payed successfully") )
          } else {
            ctx.output( orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout") )
          }
          // 清空状态
          ctx.timerService().deleteEventTimeTimer(timerTS)
          isPayedState.clear()
          timerState.clear()
        } else {
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer( value.eventTime * 1000L )
          timerState.update( value.eventTime * 1000L )
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      if(timerState.value() == timestamp){
        if( isPayedState.value()){
          // 如果pay过，说明create每来
          ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found created log"))
        } else {
          ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
        }
        isPayedState.clear()
        timerState.clear()
      }
    }
  }
}

// 自定义process function
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
  // 定义一个标记位，用于判断pay事件是否来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先取出状态
    val isPayed = isPayedState.value()

    // 判断当前数据的类型
    if( value.eventType == "create" && !isPayed ){
      // 注册一个定时器
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 900 * 1000L)
    } else if( value.eventType == "pay" ){
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    if(!isPayedState.value()){
      out.collect( OrderResult(ctx.getCurrentKey, "order timeout") )
    }
    // 清空状态
    isPayedState.clear()
  }
}