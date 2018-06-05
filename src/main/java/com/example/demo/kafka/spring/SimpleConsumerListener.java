package com.example.demo.kafka.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.concurrent.CountDownLatch;

/**
 * 消费者 （消息监听）
 * Created by makai on 2018/6/5.
 */
public class SimpleConsumerListener {
    private final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(id = "foo",topics = "spring-topic-test")
    public void listen(ConsumerRecord records){
        System.out.println("----------------------------"+records.value());
        this.latch.countDown();
    }
}
