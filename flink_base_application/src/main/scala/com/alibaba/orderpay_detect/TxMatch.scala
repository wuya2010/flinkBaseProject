package com.alibaba.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/28 14:16
  */

// 定义到账信息样例类
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long)

object TxMatch {
  // 定义侧输出流标签
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单支付数据源，包装成样例类
    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderResource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId != "")
      .keyBy(_.txId)
    // 2. 读取到账信息数据源
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent( dataArray(0), dataArray(1), dataArray(2).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 3. 连接两条流，进行处理
    val processedStream = orderEventStream.connect( receiptEventStream )
      .process( new TxPayMatch() )

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatched pays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatched receipts")

    env.execute("tx match job")
  }

  // 自定义CoProcessFunction
  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
    // 定义状态，用于保存已经来的事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 先取出状态
      val receipt = receiptState.value()
      if ( receipt != null ){
        // 如果已经有receipt到了，那么正常输出匹配
        out.collect( (pay, receipt) )
        receiptState.clear()
      } else {
        // 如果receipt还没到，那么保存pay进状态，注册一个定时器等待
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 先取出状态
      val pay = payState.value()
      if ( pay != null ){
        // 如果已经有pay到了，那么正常输出匹配
        out.collect( (pay, receipt) )
        payState.clear()
      } else {
        // 如果pay还没到，那么保存receipt进状态，注册一个定时器等待
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 3000L )
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if( payState.value() != null ){
        // 如果payState没有被清空，说明对应的receipt没到
        ctx.output( unmatchedPays, payState.value() )
      }
      if( receiptState.value() != null ){
        // 如果receiptState没有被清空，说明对应的pay没到
        ctx.output( unmatchedReceipts, receiptState.value() )
      }
      payState.clear()
      receiptState.clear()
    }
  }
}

