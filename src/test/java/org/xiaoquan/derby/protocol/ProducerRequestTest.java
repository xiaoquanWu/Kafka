package org.xiaoquan.derby.protocol;

import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.client.KafkaConfiguration;
import org.xiaoquan.derby.client.KafkaSocket;
import org.xiaoquan.derby.codec.decoder.ProducerResponseDecoder;

public class ProducerRequestTest {
    public static void main(String[] args) {
//        Properties properties = new Properties();
//        properties.setProperty("request.producer.required.ack", "1");

        KafkaConfiguration.PRODUCER_REQUIRED_ACK = (short)1;

        ProducerRequest producerRequest = new ProducerRequest();

        producerRequest.addMessage("test2", 0, new Record("test2", "Hello xiaoquan-test01"));
        producerRequest.addMessage("test02",1, new Record("test02", "Hello xiaoquan-test02"));
        producerRequest.addMessage("test02",0, new Record("test02", "Hello xiaoquan-test02"));

        KafkaSocket kafkaSocket = new KafkaSocket("127.0.0.1", 9093);
        Response response = kafkaSocket.send(producerRequest);
        response.setDecoder(new ProducerResponseDecoder());

        ProducerResponse producerResponse = (ProducerResponse)response.decodeResponse();

        System.out.println(producerResponse);
    }

}