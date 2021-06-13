package flink_sink

import java.util.Properties

import flink_source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


/**
  * @author kylinWang
  * @data 2020/7/11 19:08
  *
  */
object kafkaSink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //配置信息
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop105:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //api.common.serialization.SimpleStringSchema
    //import api.common.serialization.SimpleStringSchema
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))


    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      })

    println(dataStream)

    //反写到kafka  生产者: Semantic.EXACTLY_ONCE
    dataStream.addSink(new FlinkKafkaProducer011[String]("sinkTest",new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),properties,Semantic.EXACTLY_ONCE))


    env.execute("flink kafka")


  }
}
