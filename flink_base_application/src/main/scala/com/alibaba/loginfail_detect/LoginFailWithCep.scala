package com.alibaba.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//fixme： 定义流数据中的相邻数据执行
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据源
    val resource = getClass.getResource("/LoginLog.csv")
    //    val dataStream = env.readTextFile(resource.getPath)
    val dataStream = env.socketTextStream("192.168.25.229", 7777)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)

    // 2. 定义一个模式
    val loginFailPattern = Pattern.begin[LoginEvent]("start").where(_.status == "fail") // 定义第一个失败事件模式
      .next("next").where(_.status == "fail") // 第二个登录失败事件
      .within(Time.seconds(5))

    // 3. 将模式应用到数据流上
    val patternStream = CEP.pattern(dataStream, loginFailPattern)

    // 4. 从pattern stream中检出符合规则的事件序列，做处理
    val loginFailWarningStream = patternStream.select(new LoginFailDetect())

    loginFailWarningStream.print()

    env.execute("login fail with cep job")
  }
}

class LoginFailDetect() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFailEvent = map.get("start").iterator().next()
    val secondFailEvent = map.get("next").iterator().next()
    Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail 2 times")
  }
}