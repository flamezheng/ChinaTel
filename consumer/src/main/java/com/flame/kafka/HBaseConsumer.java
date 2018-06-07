package com.flame.kafka;

import com.flame.dao.HBaseDAO;
import com.flame.util.PropertyUtil;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;


public class HBaseConsumer {
    public static void main(String[] args) throws IOException, ParseException {
        //创建kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(PropertyUtil.properties);
        kafkaConsumer.subscribe(Collections.singletonList(PropertyUtil.properties.getProperty("kafka.topic")));
        //创建HbaseDao对象 (作用：写入数据)
        HBaseDAO hBaseDAO = new HBaseDAO();

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
            for (ConsumerRecord<String, String> record : records) {
                String ori = record.value();
                System.out.println(ori);
                hBaseDAO.put(ori);
            }
        }
    }
}
