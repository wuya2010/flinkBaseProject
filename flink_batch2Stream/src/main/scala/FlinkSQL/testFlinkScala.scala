package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._
import org.apache.flink.util.Collector


/**
  * @author kylinWang
  * @data 2022-01-23 22:19
  *
  */
object testFlinkScala {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val streamSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode() //fixme: StreamingMode
      .build()

    //SteamTableEnvironment
    val TableEnv = StreamTableEnvironment.create(env,streamSetting)

    //配置两个时间区间
    val inputStream1 = env.socketTextStream("hadoop105", 6666)
    val inputStream2 = env.socketTextStream("localhost", 5555)
    val dataStream1 = inputStream1
      .map(data => {
        val dataArray = data.split(",")
        //id, long, time
        UserBehavior(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timeOrder * 1000L
      }
    )


    // 定义watermarker
    val dataStream2 = inputStream2
      .map(data => {
        val dataArray = data.split(",")
        //id, long, time
        UserBehavior(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timeOrder * 1000L
      }
    )


    dataStream1.print("stream1")
    dataStream2.print("stream2")

    /**
      *  方式1: 代码实现
      *
      *  1. time在时间范围内，进行join
      *  2. 来一条输出一条关联结果
      *  3. 不存在窗口关闭的概念，只要在时间范围内，就会进行关联
      *
      * 1, 1547718205, 15
      * 1, 1547718220, 15
      * 1, 1547718205, 10
      * 2, 1547718205, 10
      */
//    val retStream = dataStream1.keyBy(_.id).intervalJoin(dataStream2.keyBy(_.id))
//      .between(Time.seconds(-5), Time.seconds(5))
//      .process(new IntervalJoinFunc())
//
//
//    retStream.print("test")


    // 没有窗口，会一直输出， 有了窗口的概念，会在窗口关闭后输出   .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    val retTest = dataStream1.map(x => (x.id, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sum(1)

    retTest.print("ret")

    //1, 1547719250, 10


    TableEnv.createTemporaryView("t1",dataStream1)
    TableEnv.createTemporaryView("t2",dataStream2)


    //从临时表读取数据： 表查询 Sql  fixme：方式2：  没有窗口概念，会一直保存状态
//    val sqlTable = TableEnv.sqlQuery("select t1.id, t2.id ,t1.temperature,t2.temperature from t1 left join t2 on t1.id = t2.id")


    /**
      * 1. 在窗口内完成关联查询
      * 2. 在时间范围内完成关联查询
      */

    //增加时间窗口， 在时间范围内进行关联  fixme: 支持关联的时间字段类型： <DATETIME_INTERVAL> - <DATETIME_INTERVAL>  用eventTime 作为时间字段
//    val  sqlTable= TableEnv.sqlQuery("select * from t1 , t2 where t1.id = t2.id  and t1.eventtime between t2.eventtime - INTERVAL '4' SECOND  AND t2.eventtime + INTERVAL '4' SECOND")
//
//    val resultStream = TableEnv.toRetractStream[(String,String,Double,Double)](sqlTable)
//
//    resultStream.print("rest")



    //    //sqlc查询2
//    val sqlTable2: Table =  TableEnv.sqlQuery(
//      """
//        |select id, temperature
//        |from inputTable
//        |where id ='sensor_1'
//      """.stripMargin)



    //结果写入输出表
    //    result.insertInto("outputTable")

    env.execute("get test")


  }

}

case class UserBehavior(id: String, timeOrder: Long, temperature: Double)
case class UserRet(id: String, timeOrder: Long, temperature: Double, temperature2: Double)

case class IntervalJoinFunc() extends ProcessJoinFunction[UserBehavior,UserBehavior,UserRet] {
  override def processElement(in1: UserBehavior, in2: UserBehavior, context: ProcessJoinFunction[UserBehavior, UserBehavior, UserRet]#Context, out: Collector[UserRet]): Unit = {
    out.collect(new UserRet(in1.id,in1.timeOrder,in1.temperature,in2.temperature))
  }
}
