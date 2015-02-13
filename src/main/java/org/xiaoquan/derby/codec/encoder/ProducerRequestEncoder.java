package org.xiaoquan.derby.codec.encoder;

import org.xiaoquan.derby.Constants;
import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.protocol.ProducerRequest;
import org.xiaoquan.derby.protocol.Request;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Created by XiaoQuan on 2015/1/5.
 */
public class ProducerRequestEncoder implements Encoder {
    @Override
    public byte[] encode(Request request) {
        if (!(request instanceof ProducerRequest)) {
            throw new IllegalStateException("The ProducerRequest is Invalid....");
        }
        ProducerRequest producerRequest = (ProducerRequest) request;
        List<ProducerRequest.RequestBody> requestBodies = producerRequest.getRequestBodies();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        try {
            //apiKey
            dataOutputStream.writeShort(producerRequest.getApiKey());
            //apiVersion
            dataOutputStream.writeShort(producerRequest.getApiVersion());
            //correlationId
            dataOutputStream.writeInt(producerRequest.getCorrelationId());
            //clientId
            writeString(dataOutputStream, producerRequest.getClientId());

            //requiredAcks
            dataOutputStream.writeShort(producerRequest.getRequiredAcks());
            //timeOut
            dataOutputStream.writeInt(producerRequest.getTimeOut());
            //topics
            dataOutputStream.writeInt(requestBodies.size());
            for (ProducerRequest.RequestBody requestBody : requestBodies) {
                //topicName
                writeString(dataOutputStream, requestBody.getTopic());
                Map<Integer, Record<String, String>> partitionMessage = requestBody.getPartitionMessage();
                dataOutputStream.writeInt(partitionMessage.size());

                Set<Integer> partitions = partitionMessage.keySet();
                for (Integer partitionId : partitions) {
                    //partition
                    dataOutputStream.writeInt(partitionId);
                    byte[] encodedMessageSet = encodeMessageSet(partitionMessage.get(partitionId));
                    //messageSetSize
                    dataOutputStream.writeInt(encodedMessageSet.length);
                    //messageSet
                    dataOutputStream.write(encodedMessageSet);
                }
            }
            dataOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] tempProducerRequestBytes = byteArrayOutputStream.toByteArray();
        ByteBuffer produceRequestBytes = ByteBuffer.allocate(4 + tempProducerRequestBytes.length);
        produceRequestBytes.putInt(tempProducerRequestBytes.length);
        produceRequestBytes.put(tempProducerRequestBytes);
        return produceRequestBytes.array();
    }

    protected byte[] encodeMessageSet(Record<String, String> record) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        //Offset:When the producer is sending messages it doesn't actually know the offset and can fill in any value here it likes
        dataOutputStream.writeLong(0L);
        byte[] encodedMessage = encodeMessage(Constants.MESSAGE_COMPRESSION, stringToBytes(record.getKey()), stringToBytes(record.getValue()));
        dataOutputStream.writeInt(encodedMessage.length);
        dataOutputStream.write(encodedMessage);
        dataOutputStream.close();
        return byteArrayOutputStream.toByteArray();

    }

    protected byte[] encodeMessage(short compression, byte[] key, byte[] value) throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        //magicByte
        dataOutputStream.writeByte((short) 0);
        //attribute
        dataOutputStream.writeByte(compression);

        //key
        dataOutputStream.writeInt(key.length);
        dataOutputStream.write(key);

        //value
        dataOutputStream.writeInt(value.length);
        dataOutputStream.write(value);
        dataOutputStream.close();
        byte[] temMessage = byteArrayOutputStream.toByteArray();

        CRC32 crc32 = new CRC32();
        crc32.update(temMessage);
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + temMessage.length);
        byteBuffer.putInt((int) crc32.getValue());
        byteBuffer.put(temMessage);

        return byteBuffer.array();

    }

    private byte[] stringToBytes(String value) throws UnsupportedEncodingException {
        return value.getBytes(Constants.CHARACTER_SET);
    }

    private void writeString(DataOutputStream dataOutputStream, String string) throws Exception {
        byte[] stringBytes = stringToBytes(string);
        dataOutputStream.writeShort((short) stringBytes.length);
        dataOutputStream.write(stringBytes);
    }

}
