package com.alibaba.service;

import com.alibaba.bean.UserBehavior;
import com.alibaba.bean.UserRank;
import com.alibaba.bean.UserWindow;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/12/25 8:55
 */
public class HotStatiscs {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*

        543462,1715,1464116,av,1511658000
        662867,2244074,1575622,pv,1511658000
        561558,3611281,965809,pv,1511658000
        894923,3076029,1879194,pv,1511658000
        834377,4541270,3738615,pv,1511658000
        315321,942195,4339722,pv,1511658000
        625915,1162383,570735,pv,1511658000

         */

        DataStreamSource<String> dataStream = env.socketTextStream("192.168.25.229", 7777);

        //获取输入流
        SingleOutputStreamOperator<UserBehavior> inputStream = dataStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                if (split.length == 5) {
                    //获取对象，==> 新建
                    UserBehavior userBehavior = new UserBehavior(Long.valueOf(split[0].trim()), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                    return userBehavior;
                } else {
                    return null;
                }
            }
        }).filter(new FilterFunction<UserBehavior>() {
            // 过滤行为数据是pv的数据 behavior
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                Boolean flag = false;
                if (value != null) {
                    if ("pv".equals(value.getBehavior())) {
                        flag = true;
                    }
                }
                return flag;
            }
        });

        //设置窗口时间 过时时间1s, 设置watermakerkStrategy ==> 要增加类型 <UserBehavior>
        SingleOutputStreamOperator<UserBehavior> markerStream = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                //需要上下文信息
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    //抽取时间
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                }));



        //返回什么类型？
        SingleOutputStreamOperator<UserWindow> processStream = markerStream.keyBy(UserBehavior::getBehavior)
//                (Time.seconds(10),Time.hours(1))
                // timeWindow 取消 ，需要用 Window， 或者  TumblingEventTimeWindows + processingTimeWindows
                // fixme: 具体实现？
                // 获取窗口数据 ==》 加窗口 fixme: 原 timeWindowAll 废弃， 用 windowAll ；  原 timeWindow 废弃 ，用 window
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .allowedLateness(Time.minutes(1))
//                .max("UserId");

                /**
                 *  window 之后可以进行的处理:
                 *
                 *  apply 和 process方法的区别，继承的类不同，其中prcess方法包含context，里面可以获取窗口时间，自定义延迟数据， 实现功能是相似的
                 *
                 *      +  包括 process 对窗口元素的处理 ProcessWindowFunction ， 返回值 SingleOutputStreamOperator
                 *      +  aggregae 窗口聚合 使用聚合函数 AggregateFunction   ， 返回值 SingleOutputStreamOperator
                 *      +  apply 使用 WindowFunction ; 返回值 SingleOutputStreamOperator
                 */

//                .process(new myProcess()); // test> UserWindow(windowStart=2017-11-26 09:00:00.0, windowEnd=2017-11-26 09:00:10.0, total_size=a, total_size2=b, size=3

                /** 窗口左闭右开
                 test> UserWindow(windowStart=2017-11-26 09:00:00.0, windowEnd=2017-11-26 09:00:10.0, total_size=a, total_size2=b, size=3)
                 test> UserWindow(windowStart=2017-11-26 09:00:10.0, windowEnd=2017-11-26 09:00:20.0, total_size=a, total_size2=b, size=1)
                 */
//                .apply(new myApply());



                /*
                  获取聚合结果
                    test> UserWindow(windowStart=2017-11-26 09:00:00.0, windowEnd=2017-11-26 09:00:10.0, total_size=a, total_size2=b, size=3)
                    test> UserWindow(windowStart=2017-11-26 09:00:20.0, windowEnd=2017-11-26 09:00:30.0, total_size=a, total_size2=b, size=2)
                 */
                .aggregate(new myAgg(), new myWindow());



        // 根据新的类进行分组 ==> 完成分组
        SingleOutputStreamOperator<String> retStream = processStream.keyBy(UserWindow::getWindowEnd)
                .process(new myKeyedProcess());


        // 输出最大的 max 值
        retStream.print("test");

        env.execute("mine");


    }

    /**
     * 具体实现窗口 ： 获取窗口内的元素个数
     */
    //<IN, OUT, KEY, W extends Window> ==> TimeWindow 包含 start  + end
    private static class myProcess extends ProcessWindowFunction<UserBehavior,UserWindow,String,TimeWindow> {
        @Override // s ==> key
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserWindow> out) throws Exception {
            //构建包含窗口信息的样例
            Timestamp windowStart = new Timestamp(context.window().getStart());
            Timestamp windowEnd = new Timestamp(context.window().getEnd());
            Iterator<UserBehavior> hasIter = elements.iterator();
            int count = 0;
            while(hasIter.hasNext()){
                UserBehavior next = hasIter.next();
                count ++;
            }
            out.collect(new UserWindow(windowStart, windowEnd,"a","b",count));
        }
    }



    //对窗口的处理 数据类型 ==》 <IN, OUT, KEY, W extends Window>  ==> 继承不同的 windowFunc
    private static class myApply implements WindowFunction<UserBehavior, UserWindow, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, Iterable<UserBehavior> elements, Collector<UserWindow> out) throws Exception {

            Timestamp windowStart = new Timestamp(window.getStart());
            Timestamp windowEnd = new Timestamp(window.getEnd());
            Iterator<UserBehavior> hasIter = elements.iterator();
            int count = 0;
            while(hasIter.hasNext()){
                UserBehavior next = hasIter.next();
                count ++;
            }
            out.collect(new UserWindow(windowStart, windowEnd,"a","b",count));

        }
    }


    /**
     * AggregateFunction 增量聚合函数
     *
     * 中间聚合状态： <IN, ACC, OUT>
     */
    private static class myAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 来一条增加一个结果
        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // <IN, OUT, KEY, W extends Window> ==> key 是keyby的时候的字段类型
    private static class myWindow implements WindowFunction<Long, UserWindow, String, TimeWindow> {
        // fixme: 输入 input 是一个具体的值
        @Override
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UserWindow> out) throws Exception {
            Timestamp windowStart = new Timestamp(window.getStart());
            Timestamp windowEnd = new Timestamp(window.getEnd());
            out.collect(new UserWindow(windowStart,windowEnd,"a","b",input.iterator().next().intValue()));
        }
    }

    /**
     * <K, I, O>
     *
     *  对状态的管理 : 窗口内的所有元素
     *
     *  状态保存介质：list
     *
     *  只保留了一个值的状态，没有其他
     */
    private static class myKeyedProcess extends KeyedProcessFunction< Timestamp , UserWindow, String>{

        ListState<UserWindow> listState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            // String name, Class<T> elementTypeClass ==》 获取初始状态值
            listState = getRuntimeContext().getListState(new ListStateDescriptor<UserWindow>("my_state",UserWindow.class));
        }

        // 每一条元素过来进行处理
        @Override
        public void processElement(UserWindow value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            // 注册一个时间定时器 将窗口时间设置为 闹钟时间
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd().getTime());
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            /**
             * 1. 将每次的值放入状态表中，并获取状态，进行相关排序
             * 2. 时间到达窗口结束时间
             */
           // 定义一个容器收集结果
            ArrayList<UserWindow> retList = new ArrayList<>();
            //获取状态中的值，并置空
            Iterable<UserWindow> listStateValue = listState.get();
            listStateValue.forEach(value -> retList.add(value));
            listState.clear();

            //对retlist中的值进行排序
            retList.sort(Comparator.comparing(UserWindow::getSize).reversed());
            List<UserWindow> sortList = retList.subList(0,3); // 获取前3个元素

            //结果输出， 拆分后进行打印
            StringBuffer retStr = new StringBuffer();
            for (int i = 0; i < sortList.size(); i++) {
                UserWindow elem = sortList.get(i);
                UserRank rank = new UserRank(elem.getWindowEnd(), elem.getSize());
                retStr.append(rank.toString());
            }

            //输出结果
            out.collect(retStr.toString());

        }
    }

}
