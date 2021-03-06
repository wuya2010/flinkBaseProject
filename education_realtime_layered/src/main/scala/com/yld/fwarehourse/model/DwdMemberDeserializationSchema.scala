package com.alibaba.education.model

import com.google.gson.Gson
import main.scala.com.yld.fwarehourse.bean.{DwdMember, TopicAndValue}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

// dwdmember
class DwdMemberDeserializationSchema extends KafkaDeserializationSchema[DwdMember] {

    override def isEndOfStream(nextElement: DwdMember): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): DwdMember = {
    val gson = new Gson()
    gson.fromJson(new String(record.value(), "utf-8"), classOf[DwdMember])
  }
  override def getProducedType: TypeInformation[DwdMember] = {
    TypeInformation.of(new TypeHint[DwdMember] {})
  }
}
