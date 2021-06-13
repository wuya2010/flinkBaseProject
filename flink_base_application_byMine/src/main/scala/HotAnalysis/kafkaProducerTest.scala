package scala.HotAnalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource


/**
  * @author kylinWang
  * @data 2020/7/12 22:38
  *
  */
object kafkaProducerTest {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
     properties.setProperty("bootstrap.servers","hadoop105:9092")
     properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
     properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //创建生产账
    val producer = new KafkaProducer[String,String](properties)

    //读取数据
    val fileSource: BufferedSource = io.Source.fromFile("E:\\01_myselfProject\\spark_flink_project\\flink_project\\src\\main\\resources\\UserBehavior.csv")
    for( line <- fileSource.getLines()){
        val record = new ProducerRecord[String,String]("topic",line)
        //每一条数据发送
        producer.send(record)
    }
    producer.close()
  }
}
