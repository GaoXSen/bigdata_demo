package com.gaosen.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTest implements Runnable{

    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private final String topic;
    private static final String GROUPID = "groupB";

    public KafkaConsumerTest(String topicName){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "127.0.0.1:9092");
        prop.put("group.id", GROUPID);
        prop.put("max.poll.records", 3000);
        prop.put("key.deserializer", StringDeserializer.class.getName());
        prop.put("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, String>(prop);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));


    }

    @Override
    public void run() {
        int messageNo = 1;
        System.out.println("----------开始消费----------");
        try {
            for(;;){
                msgList = consumer.poll(1000);
                if(null != msgList && msgList.count() > 0){
                    for(ConsumerRecord<String, String> record : msgList){
                        // 消费100条就打印 ,但打印的数据不一定是这个规律的
                        if(messageNo%100 == 0){
                            System.out.println(messageNo + "=======recive: key = " + record.key() + ", value = " + record.value() + " offset===" + record.offset());
                        }

                        //当消费了1000条就退出
                        if(messageNo%1000==0){
                            break;
                        }
                        messageNo++;
                    }
                }else {
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }


    public static void main(String[] args) {
        KafkaConsumerTest test1 = new KafkaConsumerTest("kafka_test");
        Thread thread = new Thread(test1);
        thread.start();

    }
}
