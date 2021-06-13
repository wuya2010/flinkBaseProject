package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {

  // 输入的登录事件样例类
  case class LoginEvent( userId: Long, ip: String, status: String, eventTime: Long )
  // 输出的报警信息样例类
  case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String )

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据源
    val resource = getClass.getResource("/LoginLog.csv")
    //    val dataStream = env.readTextFile(resource.getPath)
    val dataStream = env.socketTextStream("localhost", 7777)
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
      .next("next").where(_.status == "fail")  // 第二个登录失败事件
      .within(Time.seconds(5)) // 5s内失败2次

    // 3. 将模式应用到数据流上
    val patternStream = CEP.pattern(dataStream, loginFailPattern)

    env.execute("login fail with cep job")
  }
}