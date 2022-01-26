package com.alibaba.service;


import com.alibaba.config.CreateJDBCInputFormat;
import com.alibaba.config.Meeting;
import com.alibaba.fastjson.JSON;
import com.alibaba.sinks.SinkToGreenplum;
import com.alibaba.utils.JsonFilter;
import com.alibaba.utils.KafkaConfigUtil;
import com.alibaba.utils.Tuple2ToMeeting;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/12/23 11:24
 */
public class flinkEtlDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置表环境
        EnvironmentSettings tabSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env,tabSetting);

        tabEnv.getConfig().setIdleStateRetentionTime(Time.days(1),Time.days(2));


        /**
         * kafka 配置
         */
        Properties kafkaProp = KafkaConfigUtil.buildKafkaProps();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("", new SimpleStringSchema(), kafkaProp);

        //设置成流
        DataStreamSource<String> dataStream = env.addSource(kafkaConsumer);

        //对流进行过滤处理
        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> filterStream  = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return new JsonFilter().getJsonFilter(s);
            }
        }).map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return new JsonFilter().dataMap(s);
            }

            //获取返回值 tuple
        }).map(new MapFunction<String, Tuple5<Integer, String, Integer, String, String>>() {

            @Override
            public Tuple5<Integer, String, Integer, String, String> map(String s) throws Exception {
                return new JsonFilter().fieldMap(s);
            }
        });


        // 将流数据注册为表, 并说明表名
        tabEnv.createTemporaryView("tableName",filterStream);

        //读取mysql 的配置, 生成输入流
        CreateJDBCInputFormat createJDBCFormat=new CreateJDBCInputFormat();
        JDBCInputFormat jdbcInputFormat=createJDBCFormat.createJDBCInputFormat();

        DataStreamSource<Row> inputDataStream = env.createInput(jdbcInputFormat);
        tabEnv.createTemporaryView("tableName_2",inputDataStream);

        //维表的关联
        Table meeting_info = tabEnv.from("tableName");
        Table meeting_address = tabEnv.sqlQuery("SELECT * FROM tableName");

        String query2="SELECT mi.meeting_id, mi.meeting_code,ma.meetingroom_id,ma.meetingroom_name,ma.location_name,ma.city" +
                " FROM meeting_info AS mi " +
                "LEFT JOIN " +
                "meeting_address AS ma " +
                "ON mi.address_id=ma.meetingroom_id";

        Table joinTable = tabEnv.sqlQuery(query2);


        /**
         对结果表进行查询,TO_TIMESTAMP是Flink的时间函数，对时间格式进行转换，具体请看官网
         只对开始的会议进行转换。   统计空置率指的是统计当下时间里，已经在会议中的会议室，还是已经预定的呢
         Table joined=tEnv.sqlQuery("select meeting_id, meeting_code,TO_TIMESTAMP(mstart_date),TO_TIMESTAMP(mend_date),proctime.proctime " +
         "from meeting_info " +
         "where TO_TIMESTAMP(mstart_date)<LOCALTIMESTAMP<TO_TIMESTAMP(mend_date)");

         SQL解析过程
         String explanation = tEnv.explain(joined);
         System.out.println(explanation);

         适用于维表查询的情况1
         DataStream<Tuple2<Boolean,Row>> stream1 =tEnv.toRetractStream(joined,Row.class).filter(new FilterFunction<Tuple2<Boolean, Row>>() {
        @Override
        public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
        return booleanRowTuple2.f0;
        }
        });
         stream1.print();
         */


        //数据输出 ==> 指定样例类 Row.class
        DataStream<Tuple2<Boolean, Row>> stream_tosink = tabEnv.toRetractStream(joinTable, Row.class);

        // 对流的数据进行设置
        stream_tosink.process(new ProcessFunction<Tuple2<Boolean, Row>, Object>() {

            private Tuple2<Boolean, Row> booleanRowTuple2;
            private ProcessFunction<Tuple2<Boolean, Row>, Object>.Context context;
            private Collector<Object> collector;

            @Override
            public void processElement(Tuple2<Boolean, Row> value, Context ctx, Collector<Object> out) throws Exception {
                    if(booleanRowTuple2.f0){
                        System.out.println(JSON.toJSONString(booleanRowTuple2.f1));
                    }
            }
        });


        stream_tosink.print();

        SingleOutputStreamOperator<Meeting> retStream = stream_tosink.map(new MapFunction<Tuple2<Boolean, Row>, Meeting>() {
            @Override
            public Meeting map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
               return new Tuple2ToMeeting().getTuple2ToMeeting(booleanRowTuple2);
            }
        });


        //数据输出
        dataStream.print();
//        dataStream.addSink(new SinkToGreenplum());
        env.execute("tset");

    }

}
