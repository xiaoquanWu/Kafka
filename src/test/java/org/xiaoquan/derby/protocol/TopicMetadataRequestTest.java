package org.xiaoquan.derby.protocol;

import org.xiaoquan.derby.client.KafkaSocket;
import org.xiaoquan.derby.codec.decoder.TopicMetadataResponseDecoder;
import org.xiaoquan.derby.meta.MetaData;

import java.util.Properties;

public class TopicMetadataRequestTest {

    public static void main(String[] args) {
        Properties properties = new Properties();

        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest();
//        topicMetadataRequest.addTopic("test2");
//        topicMetadataRequest.addTopic("test02");
        topicMetadataRequest.addTopic("test03");

        KafkaSocket kafkaSocket = new KafkaSocket("127.0.0.1", 9093);
        Response response = kafkaSocket.send(topicMetadataRequest);
        response.setDecoder(new TopicMetadataResponseDecoder());

        TopicMetadataResponse topicMetadataResponse = (TopicMetadataResponse)response.decodeResponse();

        MetaData metaData = topicMetadataResponse.getMetaData();
        System.out.println(metaData);

    }

}