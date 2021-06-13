package flink_sink

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @author kylinWang
  * @data 2020/7/12 10:20
  *
  */
object RedisSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop105") //指定端口号
      .setPort(6379)
      .build()

    //获取数据源
    val inputStream = env.readTextFile("E:\\01_myselfProject\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.txt")

    //转换
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    dataStream.print()

    //这里没有结果
    dataStream.addSink(new RedisSink[SensorReading]( config, new MyRedisMapper() ))

    env.execute("redis flink")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{

  //获取描述
  override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription( RedisCommand.HSET, "sensor_temperature")
  }

  //得到数据格式
  override def getValueFromData(data: SensorReading): String = {
      data.temperature.toString
  }


  override def getKeyFromData(data: SensorReading): String = {
    println("写入redis...")
    data.id
  }
}
