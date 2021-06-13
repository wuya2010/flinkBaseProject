package flink_bulid

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * @author kylinWang
  * @data 2020/6/30 22:56
  *     建立 flink 的流数据, 建立流数据的环境
  */

object flink_source {

  def main(args: Array[String]): Unit = {

    //导包： org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
      val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置参数方式： --host hadoop105  --port 7777
      val paramTool = ParameterTool.fromArgs(args)//这个参数怎么传？
      val host = paramTool.get("host")
      val port = paramTool.getInt("port") // 获取 Int 类型
      val dataStream = env.socketTextStream(host,port)

      //env 监控 端口 : DataStreamSource[String]
//      val dataStream =  env.socketTextStream("hadoop105", 7777)

      import org.apache.flink.streaming.api.scala._

      dataStream.print()

//输出格式：
//    1> 55
//    11> nn
//    12> kk
//    2> 22

   val ds = dataStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    ds.print()

    env.execute("flink")
  }
}
