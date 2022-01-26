package com.alibaba.utils;

import lombok.extern.log4j.Log4j;

import java.util.Properties;

/**
 * Kafka配置文件
 * */

@Log4j
public class KafkaConfigUtil {
    public static String topic="test";//Kafka的topic
    public static String fieldDelimiter = ",";//字段分隔符，用于分隔Json解析后的字段

    public static Properties buildKafkaProps(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092");
        properties.setProperty("zookeeper.connect", "192.168.18.148:2181,192.168.18.149:2181,192.168.18.150:2181");
        properties.setProperty("group.id", "meeting_group3");//
        properties.put("auto.offset.reset", "latest");

      /**  earliest
        当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
                latest
        当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
                none
        topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        */

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        log.info("get kafka config, config map-> " + properties.toString());

        return properties;
    }
}
