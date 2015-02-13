package org.xiaoquan.derby.protocol;

import org.xiaoquan.derby.client.KafkaSocket;
import org.xiaoquan.derby.codec.decoder.OffsetResponseDecoder;

public class OffsetRequestTest {

    public static void main(String[] args) {

        OffsetRequest offsetRequest = new OffsetRequest();
//        offsetRequest.add("test02", 0, -1);
//        offsetRequest.add("test02", 1, -1, 2);
//        offsetRequest.add("test2", 0);
        offsetRequest.add("test03", 0);
        offsetRequest.add("test03", 1);
        offsetRequest.add("test03", 2);
        KafkaSocket kafkaSocket = new KafkaSocket("127.0.0.1", 9093);
        Response response = kafkaSocket.send(offsetRequest);
        response.setDecoder(new OffsetResponseDecoder());
        OffsetResponse offsetResponse = (OffsetResponse)response.decodeResponse();
        System.out.println(offsetResponse);

    }

}