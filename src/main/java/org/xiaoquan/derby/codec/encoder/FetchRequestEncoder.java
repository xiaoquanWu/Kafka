package org.xiaoquan.derby.codec.encoder;

import com.google.common.collect.ListMultimap;
import org.xiaoquan.derby.codec.CodeUtils;
import org.xiaoquan.derby.protocol.FetchRequest;
import org.xiaoquan.derby.protocol.OffsetRequest;
import org.xiaoquan.derby.protocol.Request;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/31.
 */
public class FetchRequestEncoder implements Encoder {
    @Override
    public byte[] encode(Request request) {
        if (!(request instanceof FetchRequest)) {
            throw new IllegalStateException("The ProducerRequest is Invalid....");
        }
        FetchRequest fetchRequest = (FetchRequest)request;
        ListMultimap<String, FetchRequest.RequestBody> topicPartitionFetchMap = fetchRequest.getTopicPartitionFetchMap();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            //apiKey
            dataOutputStream.writeShort(fetchRequest.getApiKey());
            //apiVersion
            dataOutputStream.writeShort(fetchRequest.getApiVersion());
            //correlationId
            dataOutputStream.writeInt(fetchRequest.getCorrelationId());
            //clientId
            CodeUtils.writeString(dataOutputStream, fetchRequest.getClientId());

            //replicaId
            dataOutputStream.writeInt(fetchRequest.getReplicaId());
            //maxWaitTime
            dataOutputStream.writeInt(fetchRequest.getMaxWaitTime());
            //minBytes
            dataOutputStream.writeInt(fetchRequest.getMinBytes());

            //topics
            Set<String> topics = topicPartitionFetchMap.keySet();
            dataOutputStream.writeInt(topics.size());
            for (String topic : topics) {
                CodeUtils.writeString(dataOutputStream, topic);
                List<FetchRequest.RequestBody> partitionBodies = topicPartitionFetchMap.get(topic);
                dataOutputStream.writeInt(partitionBodies.size());
                //partition
                for (FetchRequest.RequestBody partitionBody : partitionBodies) {
                    //partitionId
                    dataOutputStream.writeInt(partitionBody.getPartitionId());
                    //fetchOffset
                    dataOutputStream.writeLong(partitionBody.getFetchOffset());
                    //maxBytes
                    dataOutputStream.writeInt(partitionBody.getMaxBytes());
                }
            }
            dataOutputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] tempFetchRequestBytes = byteArrayOutputStream.toByteArray();
        ByteBuffer fetchRequestBytes = ByteBuffer.allocate(4 + tempFetchRequestBytes.length);
        fetchRequestBytes.putInt(tempFetchRequestBytes.length);
        fetchRequestBytes.put(tempFetchRequestBytes);
        return fetchRequestBytes.array();
    }
}
