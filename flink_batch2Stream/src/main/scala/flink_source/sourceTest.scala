package flink_source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011




/**
  * @author kylinWang
  * @data 2020/7/11 17:26
  *
  */
object sourceTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //返回值： DataStream[SensorReading]
    val list_ds = getListSource(env)

    //从path 获取ds
    val file_ds = getFileSource(env, "E:\\01_myselfProject\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.txt")

    //读取匹配文件
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop105:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //从 kafka 获取数据
    val kafka_ds = getKafkaSource(env, properties)


    //执行
    env.execute("test_list")
  }


  //获取flink 数据源
  def getListSource(env: StreamExecutionEnvironment) = {
    val stream: DataStream[SensorReading] = env.fromCollection(Seq(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))

    //   打印结果：
    //    9> SensorReading(sensor_1,1547718199,35.8)
    //    11> SensorReading(sensor_7,1547718202,6.7)
    //    12> SensorReading(sensor_10,1547718205,38.1)
    //    10> SensorReading(sensor_6,1547718201,15.4)

    stream
  }


  //从文件路径读取datasource
  def getFileSource(env: StreamExecutionEnvironment, path: String) = {
    val file_source = env.readTextFile(path)
    file_source
  }


  //从kafka读取datasource
  def getKafkaSource(env: StreamExecutionEnvironment, prop: Properties) = {
    //topic , valueDeserializer , props
    // simplieStringschema : Creates a new SimpleStringSchema that uses "UTF-8" as the encoding.

    //      val kafka_source = env.addSource(new FlinkKafkaConsumer011[SensorReading]("sensor", new SimpleStringSchema(), prop))
    //      kafka_source
  }
}

//自定义source
class MySource extends SourceFunction[SensorReading]{

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = ???

  override def cancel(): Unit = ???

}


case class SensorReading( id: String, timestamp: Long, temperature: Double )