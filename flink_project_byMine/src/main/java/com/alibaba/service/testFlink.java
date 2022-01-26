package com.alibaba.service;


import com.alibaba.bean.UserBehavior;
import com.alibaba.bean.resultTable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author kylinWang
 * @data 2022-01-23 21:05
 */
public class testFlink {

    public static void main(String[] args) throws Exception {

        //新建 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> inputStream1 = env.socketTextStream("hadoop105", 4444);
        DataStreamSource<String> inputStream2 = env.socketTextStream("hadoop105", 5555);


         /*

        543462,1715,1464116,av,1511658000
        662867,2244074,1575622,pv,1511658000
        561558,3611281,965809,pv,1511658000
        894923,3076029,1879194,pv,1511658000
        834377,4541270,3738615,pv,1511658000
        315321,942195,4339722,pv,1511658000
        625915,1162383,570735,pv,1511658000

         */

        //如何join
        SingleOutputStreamOperator<UserBehavior> stream1 = inputStream1.map(new MapFunction<String, UserBehavior>() {
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
        });


        SingleOutputStreamOperator<UserBehavior> stream2 = inputStream2.map(new MapFunction<String, UserBehavior>() {
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
        });

        EnvironmentSettings streamSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        //根据两条流的数据进行关联, 将流转换为table
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,streamSetting);

        tableEnv.createTemporaryView("table1",stream1);
        tableEnv.createTemporaryView("table2",stream2);

        Table tableRet1 = tableEnv.sqlQuery("select * from table1");
        DataStream<Tuple2<Boolean, UserBehavior>> retStream = tableEnv.toRetractStream(tableRet1, UserBehavior.class);
        retStream.print("test");

        //
//        Table result = tableEnv.sqlQuery("select t1.UserId,t2.UserId, t1.behavior,t2.behavior  from table1  t1 left join table2 t2 on t1.UserId = t2.UserId");
//
//        //  Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.  fixme: toAppendStream 无法使用
//
//        DataStream<Tuple2<Boolean, Row>> retStream = tableEnv.toRetractStream(result, Row.class);

//        retStream.print("t1");

        env.execute("my job");

    }

}
