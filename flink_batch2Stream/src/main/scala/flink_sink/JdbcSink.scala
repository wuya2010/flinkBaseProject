package flink_sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import flink_source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * @author kylinWang
  * @data 2020/7/11 18:09
  *
  */
object JdbcSink {
  def main(args: Array[String]): Unit = {
      // 获取与 mysql 的连接
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      //设置度
      env.setParallelism(1)

      //读取数据源
      val inputStream =  env.readTextFile("E:\\01_myselfProject\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.txt")
      //导包 import org.apache.flink.streaming.api.scala._
    val dataStream = inputStream.map(row => {
      val data = row.split(",")
      //封装为样例类
      SensorReading(data(0).trim, data(1).trim.toLong, data(2).trim.toDouble)
    })

    //拆分后得结果输出
    dataStream.print()

    //将数据源写入mysql ==> 将数据写入mysql
    dataStream.addSink(new MyJdbcSink())


     env.execute("jdbc sink")
  }
}







//继承 RichSinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{

  //定义变量
  var conn:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _

//写不进去
  override def open(parameters: Configuration): Unit = {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","")
      val insertStmt = conn.prepareStatement("insert into  temperature values(?,?)")
      val updateStmt = conn.prepareStatement("update temperature set temp = ? where sensor = ?")
      println("mysql 连接成功......")
  }

  //具体执行
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //更新数据
    updateStmt.setLong(1,value.timestamp)
    updateStmt.setDouble(2,value.temperature)
    updateStmt.executeUpdate()

    //获取 getUpdateCount
    if(updateStmt.getUpdateCount == 0) {
      //插入
   // insertStmt.setString(0,value.id)
      insertStmt.setLong(1,value.timestamp)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.executeUpdate()
    }
  }

  //关闭相应的流
  override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
  }

}
