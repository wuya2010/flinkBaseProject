package flink_process


import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._

/**
  * @author kylinWang
  * @data 2020/7/12 11:04
  *
  */
object transformTest {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

      //从端口读取
      val inputStream = env.readTextFile("E:\\01_myselfProject\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    //打印方式
//    dataStream.print("show")

    //聚合处理
    val aggStream = dataStream.keyBy("id").min("temperature")
//    aggStream.print("agg")


    //分流算子
    val splitStream = dataStream.split(data =>{
        if(data.temperature > 30) Seq("high") else Seq("low")
    })

    val lowStream = splitStream.select("low")
    val highSteam = splitStream.select("high")
    val allStream = splitStream.select("high","low")

    //合并流
    val warnStream = highSteam.map(data =>
      (data.id, data.temperature)
    )

    val connectStream = warnStream.connect(lowStream)

    //变换
    val coMapStream = connectStream.map(
      warnStream => (warnStream._1, warnStream._2, "test connect"),
      lowStream => (lowStream.id, "healthy")
    )

    //union
    val all_ds = highSteam.union(lowStream, allStream)

    //打印
//    lowStream.print("低温")
//    highSteam.print("高温")
//    allStream.print("所有")

//    warnStream.print("警告")
//    coMapStream.print("合并")
    all_ds.print("all_ds")


    //执行
    env.execute("flink trans")
  }
}

