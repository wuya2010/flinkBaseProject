import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.alibaba.education.model._
import com.yld.fwarehourse.util.ParseJsonData
import main.scala.com.yld.fwarehourse.bean.{DwdMember, DwdMemberPayMoney, DwdMemberRegtype}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

//flink run -m yarn-cluster -ynm dimetl -p 12 -ys 4 -yjm 1024 -ytm 2048m -d -c com.atguigu.education.etl.DImJoinData  -yqu flink ./education-flink-online-1.0-SNAPSHOT-jar-with-dependencies.jar --group.id test --bootstrap.servers hadoop101:9092,hadoop102:9092,hadoop103:9092
//--bootstrap.servers hadoop101:9092,hadoop102:9092,hadoop103:9092 --group.id test


//todo: 双流join

object DImJoinData {
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val GROUP_ID = "group.id"
  val RETRIES = "retries"
  val TOPIC = "topic"

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //todo: 设置时间模式为事件时间


    //checkpoint设置
    env.enableCheckpointing(60000l) //1分钟做一次checkpoint
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //仅仅一次
    checkpointConfig.setMinPauseBetweenCheckpoints(30000l) //设置checkpoint间隔时间30秒
    checkpointConfig.setCheckpointTimeout(10000l) //设置checkpoint超时时间
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION) //cancel时保留checkpoint
    //设置statebackend 为rockdb
    //        val stateBackend: StateBackend = new RocksDBStateBackend("hdfs://mycluster/flink/checkpoint")
    //        env.setStateBackend(stateBackend)

    //设置重启策略   重启3次 间隔10秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)))

    //fixme: 消费者参数，配置类
    val consumerProps = new Properties()
    consumerProps.setProperty(BOOTSTRAP_SERVERS, params.get(BOOTSTRAP_SERVERS))
    consumerProps.setProperty(GROUP_ID, params.get(GROUP_ID))

    //监控3个topic， 进行join； 用到反序列化
    val dwdmemberSource = new FlinkKafkaConsumer010[DwdMember]("dwdmember", new DwdMemberDeserializationSchema, consumerProps)
    val dwdmemberpaymoneySource = new FlinkKafkaConsumer010[DwdMemberPayMoney]("dwdmemberpaymoney", new DwdMemberPayMoneyDeserializationSchema, consumerProps)
    val dwdmemberregtypeSource = new FlinkKafkaConsumer010[DwdMemberRegtype]("dwdmemberregtype", new DwdMemberRegtypeDeserializationSchema, consumerProps)
    dwdmemberSource.setStartFromEarliest()
    dwdmemberpaymoneySource.setStartFromEarliest()
    dwdmemberregtypeSource.setStartFromEarliest()

    //注册时间作为 事件时间 水位线设置为10s, 对于延迟数据的处理
    val dwdmemberStream = env.addSource(dwdmemberSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMember](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMember): Long = {
        element.register.toLong //todo: 将哪个字段作为 eventTime
      }
    })

    //创建时间作为 事件时间  水位线设置10s
    val dwdmemberpaymoneyStream = env.addSource(dwdmemberpaymoneySource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberPayMoney](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMemberPayMoney): Long = {
        element.createtime.toLong  //todo: 将哪个字段作为 eventTime
      }
    })

    ///创建时间作为 事件时间  水位线设置10s
    val dwdmemberregtypeStream = env.addSource(dwdmemberregtypeSource).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[DwdMemberRegtype](Time.seconds(10)) {
      override def extractTimestamp(element: DwdMemberRegtype): Long = {
        element.createtime.toLong
      }
    })



    //fixme: 3条流不能同时 join , 因此这里先实现 2 个流的join
//    主表： 用户表先关联注册表 以用户表为主表 用cogroup 实现left join
    val dwdmemberLeftJoinRegtyeStream = dwdmemberStream.coGroup(dwdmemberregtypeStream) //todo: coCGroup 更底层api
      .where(item => item.uid + "_" + item.dn).equalTo(item => item.uid + "_" + item.dn)//todo: 左侧流 与 右侧流的 关联
      //      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      //      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
      .window(TumblingEventTimeWindows.of(Time.minutes(10))) //基于事件事件
      .trigger(CountTrigger.of(1)) //fixme: 触发器： 来一条触发一条（这个设置要注意）
      .apply(new MemberLeftJoinRegtype) //具体定义里面怎么处理
  // todo: 需要重新指定时间戳，不然下面是关联不上的
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(10)) {
        override def extractTimestamp(element: String): Long = {
          val register = ParseJsonData.getJsonData(element).getString("register")
          register.toLong
        }
      })



    //再根据用户信息跟消费金额进行关联 用户表为主表进行 left join 根据uid和dn进行join
    val resultStream = dwdmemberLeftJoinRegtyeStream.coGroup(dwdmemberpaymoneyStream)
      .where(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val uid = jsonObject.getString("uid")
        val dn = jsonObject.getString("dn")
        uid + "_" + dn
      }).equalTo(item => item.uid + "_" + item.dn)
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .trigger(CountTrigger.of(1))
      .apply(new resultStreamCoGroupFunction)
    //    resultStream.print()

//todo: 流 join 后的结果和 hbase 进行关联---异步IO
    // 传入参数： 流的结果， 自定义异步函数， 超时时间(到hbase查询的超时时间)， 单位s,  允许并发数（10个子线程）
    val result = AsyncDataStream.orderedWait(resultStream, new DimHbaseAsyncFunction, 10l, java.util.concurrent.TimeUnit.SECONDS, 10)
    result.print()  // (input, new AsyncFunctionForMysqlJava(), 1000, TimeUnit.MICROSECONDS, 10) //（超过10个请求，会反压上游节点）
    env.execute()
  }



  //第一次关联，得到结果
  class MemberLeftJoinRegtype extends CoGroupFunction[DwdMember, DwdMemberRegtype, String] { //todo: [x,y,z] : 左侧流， 右侧流，输出
    override def coGroup(first: lang.Iterable[DwdMember], second: lang.Iterable[DwdMemberRegtype], out: Collector[String]): Unit = {
      var bl = false

      //2个迭代器： left 不可丢失数据
      val leftIterator = first.iterator()
      val rightIterator = second.iterator()

      val t = first.iterator()

      while (leftIterator.hasNext) {
        val dwdMember = leftIterator.next()
        val jsonObject = new JSONObject()

        //放数据，key-value
        jsonObject.put("uid", dwdMember.uid)
        jsonObject.put("ad_id", dwdMember.ad_id)
        jsonObject.put("birthday", dwdMember.birthday)
        jsonObject.put("email", dwdMember.email)
        jsonObject.put("fullname", dwdMember.fullname)
        jsonObject.put("iconurl", dwdMember.iconurl)
        jsonObject.put("lastlogin", dwdMember.lastlogin)
        jsonObject.put("mailaddr", dwdMember.mailaddr)
        jsonObject.put("memberlevel", dwdMember.memberlevel)
        jsonObject.put("password", dwdMember.password)
        jsonObject.put("phone", dwdMember.phone)
        jsonObject.put("qq", dwdMember.qq)
        jsonObject.put("register", dwdMember.register)
        jsonObject.put("unitname", dwdMember.unitname)
        jsonObject.put("userip", dwdMember.userip)
        jsonObject.put("zipcode", dwdMember.zipcode)
        jsonObject.put("dt", dwdMember.dt)
        jsonObject.put("dn", dwdMember.dn)

        // 代表 join 上
        while (rightIterator.hasNext) {
          val dwdMemberRegtype = rightIterator.next()
          jsonObject.put("appkey", dwdMemberRegtype.appkey)
          jsonObject.put("appregurl", dwdMemberRegtype.appregurl)
          jsonObject.put("bdp_uuid", dwdMemberRegtype.bdp_uuid)
          jsonObject.put("createtime", dwdMemberRegtype.createtime)
          jsonObject.put("isranreg", dwdMemberRegtype.isranreg)
          jsonObject.put("regsource", dwdMemberRegtype.regsource)
          jsonObject.put("websiteid", dwdMemberRegtype.websiteid)
          bl = true
          out.collect(jsonObject.toJSONString)
        }

        // 没有关联上
        if (!bl) {
          jsonObject.put("appkey", "")
          jsonObject.put("appregurl", "")
          jsonObject.put("bdp_uuid", "")
          jsonObject.put("createtime", "")
          jsonObject.put("isranreg", "")
          jsonObject.put("regsource", "")
          jsonObject.put("websiteid", "")
          out.collect(jsonObject.toJSONString)
        }
      }
    }
  }


  //第二次关联
  class resultStreamCoGroupFunction extends CoGroupFunction[String, DwdMemberPayMoney, String] {
    override def coGroup(first: lang.Iterable[String], second: lang.Iterable[DwdMemberPayMoney], out: Collector[String]): Unit = {
      var bl = false
      val leftIterator = first.iterator()
      val rightIterator = second.iterator()


      while (leftIterator.hasNext) {
        val jsonObject = ParseJsonData.getJsonData(leftIterator.next())

        while (rightIterator.hasNext) {
          val dwdMemberPayMoney = rightIterator.next()
          jsonObject.put("paymoney", dwdMemberPayMoney.paymoney)
          jsonObject.put("siteid", dwdMemberPayMoney.siteid)
          jsonObject.put("vip_id", dwdMemberPayMoney.vip_id)
          bl = true
          out.collect(jsonObject.toJSONString)
        }
        if (!bl) {
          jsonObject.put("paymoney", "")
          jsonObject.put("siteid", "")
          jsonObject.put("vip_id", "")
          out.collect(jsonObject.toJSONString)
        }
      }
    }
  }

}
