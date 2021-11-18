package com.alibaba.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  *
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.wc
  * Version: 1.0
  *
  *  2019/10/18 11:36
  */

// 流处理word count程序

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理的执行环境    Streamf:流处理
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.disableOperatorChaining()

    // 利用传入参数来指定hostname和port
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")
    // 接收一个socket文本流
    val dataStream = env.socketTextStream(host, port)

    // 对每条数据进行处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty).startNewChain()
      .map( (_, 1) )
//        .shuffle()
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print().setParallelism(1)

    // 启动executor
    env.execute("stream word count job")
  }
}
