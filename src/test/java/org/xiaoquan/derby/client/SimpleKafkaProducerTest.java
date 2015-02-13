package org.xiaoquan.derby.client;

import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.*;

public class SimpleKafkaProducerTest {

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("request.producer.required.ack", "1");
        properties.setProperty("kafka.server.topic.names", "test03");
        KafkaConfiguration.init(properties);

        final SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer();
        simpleKafkaProducer.start();
        new Thread(new Runnable() {
            @Override
            public void run() {

                int i = 0;
                while ((++i) < 200) {
                    try {
                        simpleKafkaProducer.add("test03", "test03--" + UUID.randomUUID() + "--("+i+")");
//                        simpleKafkaProducer.add("test2", "test2--" + UUID.randomUUID() + "--("+i+")");
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        Thread.sleep(1111111111111L);

    }

}