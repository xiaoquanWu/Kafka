package org.xiaoquan.derby.codec.decoder;

import com.google.common.collect.ListMultimap;
import org.xiaoquan.derby.Constants;
import org.xiaoquan.derby.protocol.FetchResponse;
import org.xiaoquan.derby.protocol.Response;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;

/**
 * Created by XiaoQuan on 2015/1/31.
 */
public class FetchResponseDecoder implements Decoder {
    @Override
    public Response decode(byte[] bytes) {
        FetchResponse fetchResponse = new FetchResponse();
        ListMultimap<String, FetchResponse.PartitionFetchResponse> topicPartitionFetchResponse = fetchResponse.getTopicPartitionFetchResponse();

        try {
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            //size
            fetchResponse.setSize(dataInputStream.readInt());
            //correlationId
            fetchResponse.setCorrelationId(dataInputStream.readInt());

            //topicNumber
            int topicNumber = dataInputStream.readInt();
            for (int i = 0; i < topicNumber; i++) {
                short topicLength = dataInputStream.readShort();
                byte[] topicBytes = new byte[topicLength];
                dataInputStream.read(topicBytes, 0, topicLength);
                String topic = new String(topicBytes, Constants.CHARACTER_SET);
                FetchResponse.PartitionFetchResponse single = new FetchResponse.PartitionFetchResponse();

                //partitionNumber
                int partitionNumber = dataInputStream.readInt();
                for (int j = 0; j < partitionNumber; j++) {
                    //partitionId
                    int partitionId = dataInputStream.readInt();
                    //errorCode;
                    short errorCode = dataInputStream.readShort();
                    //highWaterMarkOffset
                    long highWaterMarkOffset = dataInputStream.readLong();
                    //messageSetSize
                    int messageSetSize = dataInputStream.readInt();
                    if (messageSetSize == 0) {
                        break;
                    }

                    //offset
                    long offset = dataInputStream.readLong();
                    int messageSize = dataInputStream.readInt();

                    //crc
                    int crc = dataInputStream.readInt();
                    //magicByte
                    byte magic = dataInputStream.readByte();
                    byte attribute = dataInputStream.readByte();

                    int keyLength = dataInputStream.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    dataInputStream.read(keyBytes, 0, keyLength);
                    String key = new String(keyBytes, Constants.CHARACTER_SET);

                    int valueLength = dataInputStream.readInt();
                    byte[] valueBytes = new byte[valueLength];
                    dataInputStream.read(valueBytes, 0, valueLength);
                    String value = new String(valueBytes, Constants.CHARACTER_SET);

                    FetchResponse.PartitionFetchResponse partitionFetchResponse = new FetchResponse.PartitionFetchResponse();
                    partitionFetchResponse.setPartition(partitionId);
                    partitionFetchResponse.setErrorCode(errorCode);
                    partitionFetchResponse.setHighWaterMarkOffset(highWaterMarkOffset);
                    partitionFetchResponse.setMessageSetSize(messageSetSize);
                    partitionFetchResponse.setOffset(offset);
                    partitionFetchResponse.setMessageSize(messageSize);
                    partitionFetchResponse.setCrc(crc);
                    partitionFetchResponse.setMagicByte(magic);
                    partitionFetchResponse.setAttributes(attribute);
                    partitionFetchResponse.setKey(key);
                    partitionFetchResponse.setValue(value);
                    topicPartitionFetchResponse.put(topic, partitionFetchResponse);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return fetchResponse;
    }
}
