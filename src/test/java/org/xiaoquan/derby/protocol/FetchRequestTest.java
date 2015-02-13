package org.xiaoquan.derby.protocol;

import org.xiaoquan.derby.client.KafkaSocket;
import org.xiaoquan.derby.codec.decoder.FetchResponseDecoder;

public class FetchRequestTest {

    public static void main(String[] args) throws Exception{

        FetchRequest fetchRequest = new FetchRequest();
        fetchRequest.add("test03", 0, 73);

        KafkaSocket socket = new KafkaSocket("127.0.0.1", 9093);
        Response response = socket.send(fetchRequest);
        response.setDecoder(new FetchResponseDecoder());

        FetchResponse fetchResponse = (FetchResponse)response.decodeResponse();
        System.out.println(fetchResponse);

    }

}