package com.atguigu.education.model

import main.scala.com.yld.fwarehourse.bean.TopicAndValue
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

//kafkaDeserialization
class TopicAndValueDeserializationSchema extends KafkaDeserializationSchema[TopicAndValue] {
  //流是否有最后一条元素: 无界流，所以false
  override def isEndOfStream(t: TopicAndValue): Boolean = {
    false
  }

  //反序列化方法，生成一个 topicAndValue，来一条数据重新组成一条
  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
    new TopicAndValue(consumerRecord.topic(), new String(consumerRecord.value(), "utf-8"))
  }

  //告诉flink 数据类型： 写法固定
  override def getProducedType: TypeInformation[TopicAndValue] = {
    TypeInformation.of(new TypeHint[TopicAndValue] {}) //写出topicAndValue
  }
}
