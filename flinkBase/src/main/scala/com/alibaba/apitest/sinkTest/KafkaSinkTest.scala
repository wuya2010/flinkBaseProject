package com.alibaba.apitest.sinkTest

import java.util.Properties

import com.alibaba.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

/**
  *
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  *  2019/10/19 15:44
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(60000L)

    //    val inputStream = env.readTextFile("E:\\WORKS\\Mine\\flinkBaseProject\\flinkBase\\src\\main\\resources\\sensor.txt")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      })

    dataStream.addSink(new FlinkKafkaProducer011[String]("sinkTest", new KeyedSerializationSchemaWrapper(new SimpleStringSchema()), properties, Semantic.EXACTLY_ONCE))



    //./kafka-console-consumer.sh
    dataStream.print()

    env.execute("kafka sink test")
  }
}
