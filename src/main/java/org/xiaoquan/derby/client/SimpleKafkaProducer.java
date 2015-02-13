package org.xiaoquan.derby.client;

import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.codec.decoder.ProducerResponseDecoder;
import org.xiaoquan.derby.meta.MetaData;
import org.xiaoquan.derby.meta.MetaDataCache;
import org.xiaoquan.derby.protocol.ProducerRequest;
import org.xiaoquan.derby.protocol.ProducerResponse;
import org.xiaoquan.derby.protocol.Response;

import java.util.concurrent.TimeUnit;

/**
 * Created by XiaoQuan on 2015/1/16.
 */

/**
 *
 */
public class SimpleKafkaProducer {

    private ProducerRequestQueue producerRequestQueue;

    private KafkaSocket kafkaSocket;
    private boolean close = false;

    public SimpleKafkaProducer() {
        init();
    }



    private void init() {
        producerRequestQueue = new ProducerRequestQueue(KafkaConfiguration.MAX_BUFFER_BYTES, KafkaConfiguration.MIN_AVAILABLE_SIZE, KafkaConfiguration.MAX_AVAILABLE_TIME);
    }

    /**
     * 每条消息都以主题作为Key
     * @param topicName
     * @param message
     */
    public void add(String topicName, String message) {
        Record<String, String> record = new Record<String, String>(topicName, message);
        producerRequestQueue.add(record);
    }

    public void close() {
        this.close = true;
    }

    public void start() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!close) {
                    try {
                        ProducerRequest producerRequest = producerRequestQueue.poll(10, TimeUnit.SECONDS);
                        send(producerRequest);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread.setName("send-massage-thread");
        thread.start();
    }


    private void send(ProducerRequest producerRequest) {
        MetaData metaData = MetaDataCache.getInstance().get();
        MetaData.Broker broker = metaData.getBroker();
        kafkaSocket = new KafkaSocket(broker.getHost(), broker.getPort());
        Response response = kafkaSocket.send(producerRequest);
        response.setDecoder(new ProducerResponseDecoder());
        ProducerResponse producerResponse = (ProducerResponse)response.decodeResponse();
        System.out.println(producerResponse);
    }




}
