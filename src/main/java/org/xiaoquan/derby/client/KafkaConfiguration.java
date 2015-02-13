package org.xiaoquan.derby.client;

import org.xiaoquan.derby.protocol.OffsetRequest;

import java.util.Properties;

/**
 * Created by XiaoQuan on 2015/1/15.
 */
public class KafkaConfiguration {


    //application
    public static String KAFKA_BROKER = "127.0.0.1:9093"; // ["kafka.server.broker]
    public static String KAFKA_TOPIC_NAMES = "test02,test2"; //["kafka.server.topic.names"]
    public static int METADATA_REFRESH_TIME = 5; //分钟
    public static short MESSAGE_COMPRESSION = 0;//Gzip [none:0, gzip:1, snappy:2] ["message_compression"]

    //client
    public static long MAX_BUFFER_BYTES = 1024; //消息最大值 ["message.queue.max.buffer.bytes"]
    public static long MIN_AVAILABLE_SIZE = 40; // 超过这个值后，消息出队列被发送 ["message.queue.min.available.size"]
    public static int MAX_AVAILABLE_TIME = 10; // 消息在队列中停留时间不大于这个值，否则出队列发送 ["message.queue.max.available.time"]


    //metadata
    public static short METADATA_API_VERSION = 0; //["request.metadata.api.version"]
    public static int METADATA_CORRELATION_ID = 3; //default ["request.metadata.correlationId"]
    public static String METADATA_CLIENT_ID = "Metadata-DerbySoft-XQ"; //["request.meta.client.id"]


    //produce
    public static short PRODUCER_API_VERSION = 0; //["request.producer.api.version"]
    public static int PRODUCER_CORRELATION_ID = 1; //default ["request.producer.correlationId"]
    public static String PRODUCER_CLIENT_ID = "Producer-DerbySoft-XQ"; //["request.producer.client.id"]
    public static short PRODUCER_REQUIRED_ACK = 0; //["request.producer.required.ack"]
    public static int PRODUCER_TIMEOUT = 20000; //milliseconds   ["request.producer.timeout"]

    //OffsetRequest
    public static short OFFSET_REQUEST_API_VERSION = 0; // ["offset.request.api.version“]
    public static int OFFSET_REQUEST_CORRELATION_ID = 4; //["offset.correlation.id"]
    public static String OFFSET_REQUEST_CLIENT_ID = "Offset-DerbySoft-XQ";//["offset.request.client.id"]
    public static int OFFSET_REQUEST_REPLICA_ID = -2; //["offset.request.replica.id"]
    public static long OFFSET_REQUEST_DEFAULT_TIME = OffsetRequest.TIME_TYPE_LATEST; //["offset.request.default.time"]
    public static int OFFSET_REQUEST_DEFAULT_MAX_NUMBER_OF_OFFSETS = 1; //["offset.request.default.max.number.of.offsets"]

    //fetchRequest
    public static int FETCH_REQUEST_MAX_WAIT_TIME = 1000; //milliseconds  ["fetch.request.max.wait.time"]
    public static int FETCH_REQUEST_REPLICA_ID = -1; //["fetch.request.replica.id"]
    public static int FETCH_REQUEST_MIN_BYTES = 1; // ["fetch.request.min.bytes"]
    public static int FETCH_REQUEST_MAX_BYTES = 1024; // ["fetch.request.max.bytes"]
    public static short FETCH_REQUEST_API_VERSION = 0; //["fetch.request.api.version"]
    public static int FETCH_REQUEST_CORRELATION_ID = 5;//["fetch.request.correlation.id"]
    public static String FETCH_REQUEST_CLIENT_ID = "Fetch-DerbySoft-XQ"; //["fetch.request.client.id"]


    public static void init(Properties properties) {
        //application
        KAFKA_BROKER = properties.getProperty("kafka.server.broker", KAFKA_BROKER);
        KAFKA_TOPIC_NAMES = properties.getProperty("kafka.server.topic.names", KAFKA_TOPIC_NAMES);
        METADATA_REFRESH_TIME = Integer.parseInt(properties.getProperty("metadata_refresh_time", METADATA_REFRESH_TIME + ""));
        MESSAGE_COMPRESSION = Short.parseShort(properties.getProperty("message_compression", MESSAGE_COMPRESSION + ""));

        //client
        MAX_BUFFER_BYTES = Long.parseLong(properties.getProperty("message.queue.max.buffer.bytes", MAX_BUFFER_BYTES + ""));
        MIN_AVAILABLE_SIZE = Long.parseLong(properties.getProperty("message.queue.min.available.size", MIN_AVAILABLE_SIZE + ""));
        MAX_AVAILABLE_TIME = Integer.parseInt(properties.getProperty("message.queue.max.available.time", MAX_AVAILABLE_TIME + ""));


        //metadata
        METADATA_CORRELATION_ID = Integer.parseInt(properties.getProperty("request.metadata.correlationId", METADATA_CORRELATION_ID + ""));
        METADATA_API_VERSION = Short.parseShort(properties.getProperty("request.metadata.api.version", METADATA_API_VERSION + ""));
        METADATA_CLIENT_ID = properties.getProperty("request.meta.client.id", METADATA_CLIENT_ID);

        //producer
        PRODUCER_CORRELATION_ID = Integer.parseInt(properties.getProperty("request.producer.correlationId", PRODUCER_CORRELATION_ID + ""));
        PRODUCER_API_VERSION = Short.parseShort(properties.getProperty("request.producer.api.version", PRODUCER_API_VERSION + ""));
        PRODUCER_CLIENT_ID = properties.getProperty("request.producer.client.id", PRODUCER_CLIENT_ID);
        PRODUCER_REQUIRED_ACK = Short.parseShort(properties.getProperty("request.producer.required.ack", PRODUCER_REQUIRED_ACK + ""));
        PRODUCER_TIMEOUT = Integer.parseInt(properties.getProperty("request.producer.timeout", PRODUCER_TIMEOUT + ""));

        //offset
        OFFSET_REQUEST_API_VERSION = Short.parseShort(properties.getProperty("offset.request.api.version", OFFSET_REQUEST_API_VERSION + ""));
        OFFSET_REQUEST_CORRELATION_ID = Integer.parseInt(properties.getProperty("offset.correlation.id", OFFSET_REQUEST_CORRELATION_ID + ""));
        OFFSET_REQUEST_CLIENT_ID = properties.getProperty("offset.request.client.id", OFFSET_REQUEST_CLIENT_ID + "");
        OFFSET_REQUEST_DEFAULT_TIME = Long.parseLong(properties.getProperty("offset.request.default.time", OFFSET_REQUEST_DEFAULT_TIME + ""));
        OFFSET_REQUEST_DEFAULT_MAX_NUMBER_OF_OFFSETS = Integer.parseInt(properties.getProperty("offset.request.default.max.number.of.offsets", OFFSET_REQUEST_DEFAULT_MAX_NUMBER_OF_OFFSETS + ""));
        OFFSET_REQUEST_REPLICA_ID = Integer.parseInt(properties.getProperty("offset.request.replica.id", OFFSET_REQUEST_REPLICA_ID + ""));

        //fetch
        FETCH_REQUEST_REPLICA_ID = Integer.parseInt(properties.getProperty("fetch.request.replica.id", FETCH_REQUEST_REPLICA_ID + ""));
        FETCH_REQUEST_MAX_WAIT_TIME = Integer.parseInt(properties.getProperty("fetch.request.max.wait.time", FETCH_REQUEST_MAX_WAIT_TIME + ""));
        FETCH_REQUEST_MIN_BYTES = Integer.parseInt(properties.getProperty("fetch.request.min.bytes", FETCH_REQUEST_MIN_BYTES + ""));
        FETCH_REQUEST_MAX_BYTES = Integer.parseInt(properties.getProperty("fetch.request.max.bytes", FETCH_REQUEST_MAX_BYTES + ""));
        FETCH_REQUEST_API_VERSION = Short.parseShort(properties.getProperty("fetch.request.api.version", FETCH_REQUEST_API_VERSION + ""));
        FETCH_REQUEST_CORRELATION_ID = Integer.parseInt(properties.getProperty("fetch.request.correlation.id", FETCH_REQUEST_CORRELATION_ID + ""));
        FETCH_REQUEST_CLIENT_ID = properties.getProperty("fetch.request.client.id", FETCH_REQUEST_CLIENT_ID + "");

    }

}
