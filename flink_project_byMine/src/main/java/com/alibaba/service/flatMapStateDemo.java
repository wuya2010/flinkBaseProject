package com.alibaba.service;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <p>Description: 添加描述</p>
 * <p>Copyright: Copyright (c) 2020</p>
 * <p>Company: TY</p>
 *
 * @author kylin
 * @version 1.0
 * @date 2021/12/27 10:05
 */
public class flatMapStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        DataStreamSource<Tuple2<Long, Long>> inputStream = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L));


        //获取
        SingleOutputStreamOperator<Tuple2<Long, Long>> retStream = inputStream.keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage());

        inputStream.print();
        retStream.print();

        env.execute("test");

    }

    // <T, O>
    private static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

        private transient ValueState<Tuple2<Long, Long>> stateValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            //保存f0的状态
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>("my-state", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
            }), Tuple2.of(0L, 0L));

            stateValue = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            Tuple2<Long, Long> sum = stateValue.value();

            // 过来一个值，对sum 进行叠加, 获取平均值
            sum.f1 += input.f1;
            sum.f0 +=  1;

            // 更新状态 , 状态的处理方式
            stateValue.update(sum);

            // 输出后，清空状态
            if(sum.f0 >=2 ){
                out.collect(new Tuple2(sum.f0,sum.f1/sum.f0));
                stateValue.clear();
            }

        }

    }
}
