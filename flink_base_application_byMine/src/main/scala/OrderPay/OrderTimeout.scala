package com.atguigu.orderpay_detect

import java.util


import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 输入数据样例类
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
// 输出订单支付结果样例类
case class OrderResult( orderId: Long, resultMsg: String )

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据源，包装成样例类
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 2. 定义一个模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay").where(_.txId != "")
      .within(Time.minutes(15))

    // 3. 把模式应用到数据流上: 定义一个输出标签
    // 订单事件流根据 orderId 分流，然后在每一条流中匹配出定义好的模式
    val patternStream = CEP.pattern( orderEventStream, orderPayPattern )

    // 4. 用select方法获取匹配到的事件序列，并做处理
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")
    val resultStream = patternStream.select( orderTimeoutOutputTag,
      new OrderTimeOutSelect(),
      new OrderPaySelect())

    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout detect job")
  }
}



//IN, OUT
class OrderTimeOutSelect() extends  PatternTimeoutFunction[OrderEvent,OrderResult]{

  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult ={
    //这2个参数理解一下
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId,"time out")
  }
}


//<IN, OUT>
class OrderPaySelect()  extends PatternSelectFunction[OrderEvent,OrderResult]{

  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    //
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId,"payed successfully")
  }

}