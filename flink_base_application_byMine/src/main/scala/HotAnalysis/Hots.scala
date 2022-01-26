package scala.HotAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  * @author kylinWang
  * @data 2020/7/12 22:37
  *
  */
object Hots {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(5)

    /*  val properties = new Properties()
      properties.setProperty("bootstrap.servers", "localhost:9092")
      properties.setProperty("group.id", "consumer-group")
      properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      properties.setProperty("auto.offset.reset", "latest")*/

    // 读取数据
    // val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

    // 543462,1715,1464116,pv,1511658000

    val inputStream: DataStream[UserBehavior] = env.socketTextStream("192.168.7.135",7777)
      .map(data => {
        val dataArray = data.split(",")
        //UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      //增加时间标记, 创建 watermark
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 对数据进行窗口聚合处理, fixme: 输出值类型： temId: Long,  windowEnd: Long,  count: Long
    val aggStream: DataStream[ItemViewCount] = inputStream
      .filter(_.behavior == "pv") // 过滤出pv数据
      .keyBy(_.itemId)//对商品进行分组 , 各分区数据在同一个 task 中

//      .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(16)))


       // fixme : 了解窗口的关闭时间
      // 不加偏移统计的是 整数 ， 加了偏移统计零散值： agg:5> ItemViewCount(1715,1639989000000,1639992600000,1)
      .window(TumblingEventTimeWindows.of(Time.hours(1),Time.minutes(30))) //按小时统计, 会按整小时进行统计


      // window 是滑窗的简写
//      .timeWindow(Time.seconds(10), Time.seconds(5)) // 开窗进行统计， 基于哪个参数进行开窗
      .aggregate(new CountAgg(), new WindowCountResult()) // 聚合出当前商品在时间窗口内的统计数量

    /**
     * ==>  aggStream.print("agg")
     * 窗口得关闭时间
     * ItemViewCount(1715,1511657995000,1511658005000,1) ==> [08:59:55,09:00:05]
     * ItemViewCount(1715,1511658005000,1511658015000,1) ==> [2017-11-26 09:00:05,2017-11-26 09:00:15] 窗口关闭时间  2017-11-26 09:00:50
     *
     * 时间：2017-11-26 09:01:09 关闭 [2017-11-26 09:00:55,2017-11-26 09:01:05]
     *  2017-11-26 09:01:19 关闭 [2017-11-26 09:01:00,2017-11-26 09:01:10]
     */


    /**
     * 上一步聚合出结果
     * agg:1> ItemViewCount(1716,1511658005000,1511658015000,5)
     * agg:4> ItemViewCount(1717,1511658005000,1511658015000,3)
     * agg:5> ItemViewCount(1715,1511658005000,1511658015000,3)
     *
     * 传到keyby 后，不能马上出结果
     * 写入数据
     * 543463,1716,1464116,pv,1511658024
     * 543463,1717,1464116,pv,1511658024
     * 543463,1715,1464116,pv,1511658024
     *  窗口关闭：
     * process:4> 时间：2017-11-26 09:00:15.0
     * No1: 商品ID=1716 点击量=5
     * No2: 商品ID=1717 点击量=3
     *
     * fxime: 等到新一个窗口到达后，才会出结果
     */
    aggStream.print("agg")




    // 对聚合结果按照窗口分组，并排序
    //fixme:  将聚合后得结果作为流式数据源文件，进行keyby
    val processedStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(2)) // 用process function做排序处理，得到top N

    //    inputStream.print("input")
    //    aggStream.print("agg")
    processedStream.print("process")

    env.execute("Hot items job")
  }
}


//输入数据
// 数据格式 ： 543462,1715,1464116,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
//中间样例
//case class ItemViewCount(itemId: Long, windowEnd: Long,count: Long)
case class ItemViewCount(itemId: Long, windowStart:Long, windowEnd: Long,count: Long)


// 自定义的预聚合函数，来一条数据就加一 <IN, ACC, OUT>
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 示例：求取平均数
class MyAverageAgg() extends AggregateFunction[Long, (Long, Int), Double] {
  override def add(value: Long, accumulator: (Long, Int)): (Long, Int) = (accumulator._1 + value, accumulator._2 + 1)

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2.toDouble

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}






// 自定义window function  ==> IN, OUT, KEY, W <: Window]
class WindowCountResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  //  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
  //  val itemId = key.asInstanceOf[Tuple1[Long]].f0
  //    val windowEnd = window.getEnd
  //    val count = input.iterator.next()
  //    out.collect( ItemViewCount(itemId, windowEnd, count) )
  //  }
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//    out.collect(ItemViewCount(key, window.getStart, input.iterator.next()))  // 窗口关闭时间： agg:5> ItemViewCount(1715,1511658005000,2)
    out.collect(ItemViewCount(key, window.getStart,window.getEnd, input.iterator.next()))  // 窗口关闭时间： agg:5> ItemViewCount(1715,1511658005000,2)
  }
}





// 自定义process function
//实现： 1. 以窗口大小为聚合时间，对窗口内数据进行初聚合；
//2. 利用 keyProcessFunction 函数对分组后的数据，获取top数据
//3. 根据窗口设置注册时间
//4. 在满足时间触发条件后，对数据进行逻辑处理  <K, I, O>
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 定义一个列表状态，用于保存所有的商品个数统计值
  private var itemListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemList-state", classOf[ItemViewCount]))
  }

  // 流中的每一个元素都会调用这个方法，调用结果将会放在Collector 数据类型中输
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，就保存入list state，注册一个定时器
    itemListState.add(value)
    // fixme: 每一个窗口数据过来，都会生成一个 timeService()
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  // 到达触发时间: 参数timestamp为定时器所设定的触发的时间戳
  // OnTimerContext和processElement的Context参数一样，提供了上下文的一些信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 先将所有数据从状态中取出
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemListState.get()) {
      allItems += item
    }
    itemListState.clear()

    // 按照点击量从大到小排序，并取Top N
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将信息格式化为String，方便打印输出
    val results: StringBuilder = new StringBuilder()
    results.append("时间：").append(new Timestamp(timestamp)).append("\n")
    // 对排序的数据遍历输出
    for (i <- sortedItems.indices) {//相当于  sortedItems.length -1  获取下角标 fixme: sortedItems.indices
      val currentItem = sortedItems(i)
      results.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 点击量=").append(currentItem.count)
        .append("\n")
    }
    results.append("=====================================")
    Thread.sleep(1000L)
    out.collect(results.toString())
  }
}