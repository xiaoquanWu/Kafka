package org.xiaoquan.derby.client;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.codec.decoder.FetchResponseDecoder;
import org.xiaoquan.derby.protocol.FetchRequest;
import org.xiaoquan.derby.protocol.FetchResponse;
import org.xiaoquan.derby.protocol.Request;
import org.xiaoquan.derby.protocol.Response;

import java.util.Map;
import java.util.Properties;

/**
 * 只消费一个主题上的消息
 */
public class SimpleKafkaConsumer {

    private String host;
    private int port;

    private String topic;
    private Map<Integer, Long> currentConsumerPartitionOffsetMap = Maps.newHashMap();

    private Properties properties;

    private boolean close = false;

    public SimpleKafkaConsumer(Properties properties) {
        this.properties = properties;
        initConfiguration(properties);
    }

    public String consumer(Integer partition, Long offset) {
        FetchRequest fetchRequest = createFetchRequest();
        fetchRequest.add(topic, partition, offset, maxBytes);
        Map<String, HashBasedTable<Integer, Long, Record<String, String>>> topicPartitionRecords = send(fetchRequest);
        return topicPartitionRecords.get(topic).get(partition, offset).getValue();
    }

    public void start() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                while (!close) {

                }
            }
        };

        Thread thread = new Thread(runnable);
        thread.setName("Kafka-Consumer-Thread");
        thread.start();

    }


    private Map<String, HashBasedTable<Integer, Long, Record<String, String>>> send(Request request) {
        Map<String, HashBasedTable<Integer, Long, Record<String, String>>> topicPartitionRecords = Maps.newHashMap();

        KafkaSocket kafkaSocket = new KafkaSocket(host, port);
        Response response = kafkaSocket.send(request);
        response.setDecoder(new FetchResponseDecoder());
        FetchResponse fetchResponse = (FetchResponse) response.decodeResponse();
        ListMultimap<String, FetchResponse.PartitionFetchResponse> topicPartitionFetchResponse = fetchResponse.getTopicPartitionFetchResponse();
        for (String topic : topicPartitionFetchResponse.keySet()) {
            HashBasedTable<Integer, Long, Record<String, String>> partitionRecords = HashBasedTable.create();
            for (FetchResponse.PartitionFetchResponse fetchResult : topicPartitionFetchResponse.get(topic)) {
                partitionRecords.put(fetchResult.getPartition(), fetchResult.getOffset(), new Record<String, String>(fetchResult.getKey(), fetchResult.getValue()));
            }
            topicPartitionRecords.put(topic, partitionRecords);
        }
        return topicPartitionRecords;
    }

    public void close() {
        this.close = true;
    }
    private void setConsumerPosition() {
        //TODO

        //Test
        currentConsumerPartitionOffsetMap.put(0, 11900L);
        currentConsumerPartitionOffsetMap.put(1, 11800L);

    }


    private String clientId;
    private Integer correlationId;
    private Integer minBytes;
    private Integer maxWaitTime;
    private Integer maxBytes;

    private FetchRequest createFetchRequest() {
        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.setCorrelationId(this.correlationId);
        fetchRequest.setMaxWaitTime(this.maxWaitTime);
        fetchRequest.setMinBytes(this.minBytes);
        fetchRequest.setClientId(this.clientId);
        return fetchRequest;
    }

    private void initConfiguration(Properties properties) {
        this.host = properties.getProperty("kafka.consumer.host");
        String portStr = properties.getProperty("kafka.consumer.port");
        if (host == null || portStr ==  null) {
            throw new IllegalStateException("kafka.consumer.host/kafka.consumer.port must be config");
        }
        this.port = Integer.parseInt(portStr);
        this.topic = properties.getProperty("kafka.consumer.topic.name");



        this.correlationId = Integer.parseInt(properties.getProperty("kafka.consumer.correlation.id", "5"));
        this.maxWaitTime = Integer.parseInt(properties.getProperty("kafka.consumer.max.wait.time", "3000"));
        this.minBytes = Integer.parseInt(properties.getProperty("kafka.consumer.min.bytes", "0"));
        this.clientId = properties.getProperty("kafka.consumer.client.id", "kafka.consumer.client.XQ");
        this.maxBytes = Integer.parseInt(properties.getProperty("kafka.consumer.max.bytes", "1024"));

        setConsumerPosition();

    }

}
