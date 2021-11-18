package com.alibaba.education.etl

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.alibaba.education.model.{DwdKafkaProducerSerializationSchema, GlobalConfig, TopicAndValueDeserializationSchema}
import com.yld.fwarehourse.util.ParseJsonData
import main.scala.com.yld.fwarehourse.bean.TopicAndValue
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector

// 执行脚本
//flink run -m yarn-cluster -ynm odsetldata -p 12 -ys 4  -yjm 1024 -ytm 2048m -d -c com.atguigu.education.etl.OdsEtlData -yqu flink ./education-flink-online-1.0-SNAPSHOT-jar-with-dependencies.jar --group.id test --bootstrap.servers hadoop101:9092,hadoop102:9092,hadoop103:9092 --topic basewebsite,basead,member,memberpaymoney,memberregtype,membervip

//--group.id test --bootstrap.servers hadoop101:9092,hadoop102:9092,hadoop103:9092 --topic basewebsite,basead,member,memberpaymoney,memberregtype,membervip


object OdsEtlData {
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val GROUP_ID = "group.id"
  val RETRIES = "retries"
  val TOPIC = "topic"

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置时间模式为事件时间

    //checkpoint设置
    env.enableCheckpointing(60000l) //1分钟做一次checkpoint
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //仅仅一次
    checkpointConfig.setMinPauseBetweenCheckpoints(30000l) //设置checkpoint间隔时间30秒
    checkpointConfig.setCheckpointTimeout(10000l) //设置checkpoint超时时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel时保留checkpoint
    //设置statebackend 为rockdb
//    val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint")
//    env.setStateBackend(stateBackend)

    //设置重启策略  ： 重启3次 间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)))

    //todo： scala 中 转换为 java
    import scala.collection.JavaConverters._
    //拆分topic： 消费 topic 数据 ： flink 监控 6 个 topic， 形成 1 个流，通过监控名称进行区分
    val topicList = params.get(TOPIC).split(",").toBuffer.asJava
    //fixme: scala 中 split 后是Array 数组; flink 中 需要传入 list (java)

    //定义参数
    val consumerProps = new Properties()
    consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID))//todo: 传入的是什么参数
    //方法中的序列化， 消费者参数 kafka source ： 输入的数据自动转换成 topicAndValue
    val kafaEventSource = new FlinkKafkaConsumer010[TopicAndValue](topicList, new TopicAndValueDeserializationSchema, consumerProps) //TopicAndValueDeserializationSchema: 自定义反序
    kafaEventSource.setStartFromEarliest() //todo: 设置消费模式（checkepoint没有设置时，才走这个）

    //todo: kafka source
    val dataStream = env.addSource(kafaEventSource).filter(item => {
      //先过滤非json数据
      val obj: JSONObject = ParseJsonData.getJsonData(item.value)
      obj.isInstanceOf[JSONObject]
    })


    //todo: 将dataStream拆成两份 一份维度表写到hbase 另一份事实表数据写到第二层kafka
    val sideOutHbaseTag = new OutputTag[TopicAndValue]("hbaseSinkStream")
    //    val sideOutGreenPlumTag = new OutputTag[TopicAndValue]("greenplumSinkStream")
    val result = dataStream.process(new ProcessFunction[TopicAndValue, TopicAndValue] { //参数类型： 【I,O】： 输入输出类型
      override def processElement(value: TopicAndValue, ctx: ProcessFunction[TopicAndValue, TopicAndValue]#Context, out: Collector[TopicAndValue]): Unit = {
        value.topic match {
            //todo:  the side output identified 测输出流
          case "basead" | "basewebsite" | "membervip" => ctx.output(sideOutHbaseTag, value)
          case _ => out.collect(value)
        }
      }
    })
    //侧输出流得到 需要写入hbase的数据
    //        result.getSideOutput(sideOutGreenPlumTag).addSink(new DwdGreenPlumSink)

//    result.getSideOutput(sideOutHbaseTag).addSink(new DwdHbaseSink) // 侧输出流从 hbase 流出

    //    //事实表数据写入第二层kafka： 2 层 kafka
    result.addSink(new FlinkKafkaProducer010[TopicAndValue](GlobalConfig.BOOTSTRAP_SERVERS, "", new DwdKafkaProducerSerializationSchema))//自定义：序列化类
    env.execute()
  }

  class DwdHbaseSink(){

  }
}
