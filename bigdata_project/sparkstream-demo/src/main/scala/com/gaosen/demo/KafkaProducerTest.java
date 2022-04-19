package com.gaosen.demo;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author gaosen
 * @Date 2022/4/19 14:50
 * @Email gaosen@tuyoogame.com
 * @Description
 *
 */

public class KafkaProducerTest implements Runnable{

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaProducerTest(String topicName) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers","127.0.0.1:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<String, String>(prop);
        this.topic = topicName;

    }


    @Override
    public void run() {
        int messageNo = 1;
        try{
            for(;;){
                String messageStr = "你好，这是第" + messageNo + "条消息";
                producer.send(new ProducerRecord<String, String>(topic,"Message",messageStr));

                //生产了100条就打印
                if(messageNo%100 == 0){
                    System.out.println("发送的消息：" + messageStr);
                }

                //生产了1000条就退出
                if(messageNo%1000 == 0){
                    System.out.println("发送的消息：" + messageStr);
                    break;
                }
                messageNo++;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }


    public static void main(String[] args) {
        KafkaProducerTest test = new KafkaProducerTest("kafka_test");
        Thread thread = new Thread(test);
        thread.start();
    }

}
