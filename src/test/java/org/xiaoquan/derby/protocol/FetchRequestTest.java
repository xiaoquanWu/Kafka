package org.xiaoquan.derby.protocol;

import org.xiaoquan.derby.client.KafkaSocket;
import org.xiaoquan.derby.codec.decoder.FetchResponseDecoder;

import java.util.Properties;

import static org.junit.Assert.*;

public class FetchRequestTest {

    public static void main(String[] args) {

        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.add("test03", 0, 6);

        KafkaSocket socket = new KafkaSocket("127.0.0.1", 9093);
        Response response = socket.send(fetchRequest);
        response.setDecoder(new FetchResponseDecoder());

        FetchResponse fetchResponse = (FetchResponse)response.decodeResponse();
        System.out.println(fetchResponse);

    }

}