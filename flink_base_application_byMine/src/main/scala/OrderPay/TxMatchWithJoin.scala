package com.atguigu.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单支付数据源，包装成样例类
    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .filter(_.txId != "")
          .keyBy(_.txId)


    // 2. 读取到账信息数据源
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
          .keyBy(_.txId)

    // 1. window join：  join 流的join: 这种join 方式会丢掉一些数据
    orderEventStream.join(receiptEventStream) //todo: Flink stream left join
      .where(_.txId)//第一个流字段
      .equalTo(_.txId) //关联条件： 第二个流的字段
      .window( TumblingEventTimeWindows.of(Time.seconds(15)) )//滚动 + 滑动 + 绘画窗口
      .apply( (pay, receipt) => (pay, receipt) ) //只能调用 apply
//      .print()

    // 2. interval join ： 区间join， 不用window 开窗, 而是来一个数据开一个窗口
    val processedStream = orderEventStream.intervalJoin(receiptEventStream) //keyBy 之后进行join
      .between(Time.seconds(-15), Time.seconds(20))//时间的上界和下届
      .process( new TxMatchByInterjoin() )

    processedStream.print()

    env.execute("tx match job")
  }
}

class TxMatchByInterjoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(pay: OrderEvent, receipt: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect( (pay, receipt) )
  }
}