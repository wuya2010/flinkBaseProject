package com.atguigu.education.model

import java.nio.charset.Charset

import main.scala.com.yld.fwarehourse.bean.TopicAndValue
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema

// kafka 生产者
//fixme: 为什么要序列化

class DwdKafkaProducerSerializationSchema extends KeyedSerializationSchema[TopicAndValue] {
  val serialVersionUID = 1351665280744549933L;

  override def serializeKey(element: TopicAndValue): Array[Byte] = null //key置1空

  override def serializeValue(element: TopicAndValue): Array[Byte] = {
    element.value.getBytes(Charset.forName("utf-8"))
  }

  override def getTargetTopic(element: TopicAndValue): String = { //获取目标的topic
    "dwd" + element.topic  //根据原topic名称，增加前缀后，分发到其他topic
  }
}
