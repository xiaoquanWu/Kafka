package org.xiaoquan.derby.codec.decoder;

import org.xiaoquan.derby.Constants;
import org.xiaoquan.derby.protocol.ProducerResponse;
import org.xiaoquan.derby.protocol.Response;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Created by XiaoQuan on 2015/1/7.
 */
public class ProducerResponseDecoder implements Decoder {
    @Override
    public Response decode(byte[] bytes) {
        ProducerResponse producerResponse = new ProducerResponse();
        try {
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            //size
            producerResponse.setSize(dataInputStream.readInt());
            //correlationId
            producerResponse.setCorrelationId(dataInputStream.readInt());

            //topicSize
            int topicSize = dataInputStream.readInt();
            for (int i = 0; i < topicSize; i++) {
                short topicNameLength = dataInputStream.readShort();
                byte[] topicNameBytes = new byte[topicNameLength];
                dataInputStream.read(topicNameBytes, 0, topicNameLength);
                String topicName = new String(topicNameBytes, Constants.CHARACTER_SET);

                ProducerResponse.ResponseBody responseBody = new ProducerResponse.ResponseBody(topicName);
                int partitionStatusSize = dataInputStream.readInt();
                for (int j = 0; j < partitionStatusSize; j++) {
                    int partitionId = dataInputStream.readInt();
                    short errorCode = dataInputStream.readShort();
                    long offset = dataInputStream.readLong();

                    responseBody.addPartitionStatus(partitionId, errorCode, offset);
                }
                producerResponse.getResponseBodies().add(responseBody);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return producerResponse;
    }
}
