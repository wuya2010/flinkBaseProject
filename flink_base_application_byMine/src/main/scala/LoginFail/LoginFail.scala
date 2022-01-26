

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.HotAnalysis.UserBehavior


// 输入的登录事件样例类
case class LoginEvent( userId: Long, ip: String, status: String, eventTime: Long )
// 输出的报警信息样例类
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String )

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源
//    val resource = getClass.getResource("/LoginLog.csv")
//    val dataStream = env.readTextFile(resource.getPath)


    //5402,83.149.11.115,success,1558430815  ==> 测试的时候，是否每来一条就返回
    val dataStream= env.socketTextStream("192.168.7.135",9999)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(2)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )
      .keyBy(_.userId)

      /**
       * 获取特征： 时间范围内连续登录失败
       * 1. 没有定义 watermark process 无法执行；
       *输入数据
       * 5402,83.149.11.115,fail,1511658000
       * 5402,83.149.11.115,fail,1511658001
       * 5402,83.149.11.115,fail,1511658001
       * 5402,83.149.11.115,fail,1511658002
       * 5402,83.149.11.115,fail,1511658003
       * 5402,83.149.11.115,fail,1511658004
       * ==》 窗口关闭，获取结果
       * Warning(5402,1511658000,1511658004,Login fail in 5 seconds for 6 times)
       *
       * 5402,83.149.11.115,fail,1511658002
       * 5402,83.149.11.115,fail,1511658000
       * 5402,83.149.11.115,fail,1511658004
       * 5402,83.149.11.115,fail,1511658005
       * 5402,83.149.11.115,fail,1511658006
       * ==》 获取窗口
       * Warning(5402,1511658002,1511658006,Login fail in 5 seconds for 5 times)
       *
       */
      .process(new LoginFailWarning(2))



//      .process( new LoginFailWarningAdv(2) )


    dataStream.print()
    env.execute("login fail job")
  }
}



// 自定义 process function , 时间范围内连续登录失败
class LoginFailWarning(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义一个List状态，用于保存连续登录失败的事件
  lazy val loginFailListsState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 判断是否失败事件，如果是，添加到状态中，定义一个定时器
    if( value.status == "fail" ){
      loginFailListsState.add(value)
      //每次失败,注册一个定时器 , 2s内失败的次数
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 5000L)
    } else{
      loginFailListsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 判断状态列表中登录失败的个数
    import scala.collection.JavaConversions._
    val count = loginFailListsState.get().size //状态中有多少条数据

    if( count >= failTimes ){
      out.collect( Warning( ctx.getCurrentKey,
        loginFailListsState.get().head.eventTime,
        loginFailListsState.get().last.eventTime,
        "Login fail in 5 seconds for " + count + " times" ) )
    }

    /**
     * Warning(5402,1511658000,1511658005,Login fail in 5 seconds for 3 times)  ==> [2017-11-26 09:00:00, 2017-11-26 09:00:05]
     */

    // 清空状态
     loginFailListsState.clear()
  }
}






// 状态保存 <K, I, O>
class LoginFailWarningAdv(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义一个List状态，用于保存连续登录失败的事件
  // 样例类： LoginEvent( userId: Long, ip: String, status: String, eventTime: Long )
  lazy val loginFailListsState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList-state", classOf[LoginEvent]))

  // value: 输入
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 按照status筛选失败的事件，如果成功状态清空
    if( value.status == "fail" ){
      // 定义迭代器获取状态
      val iter = loginFailListsState.get().iterator()
      // 如果已经有失败事件才做处理，没有的话把当前事件直接add进去
      if( iter.hasNext ){
        val firstFailEvent = iter.next()
        // 如果两次登录失败事件间隔小于5秒，输出报警信息---内部逻辑，如果当前时间与状态中的时间差小于 5
        if( (value.eventTime - firstFailEvent.eventTime).abs < 5 ){
          out.collect( Warning( value.userId, firstFailEvent.eventTime, value.eventTime, "login fail in 5 seconds" ) )
        }
        loginFailListsState.clear()
        loginFailListsState.add(value)
      } else{
        loginFailListsState.add(value)
      }
    } else{
      loginFailListsState.clear()
    }

    // Warning(5402,1511658000,1511658002,login fail in 2 seconds) ==> 登录失败
  }
}