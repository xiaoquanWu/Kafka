package org.xiaoquan.derby.codec.encoder;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import org.xiaoquan.derby.codec.CodeUtils;
import org.xiaoquan.derby.protocol.OffsetRequest;
import org.xiaoquan.derby.protocol.ProducerRequest;
import org.xiaoquan.derby.protocol.Request;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/25.
 */
public class OffsetRequestEncoder implements Encoder {
    @Override
    public byte[] encode(Request request) {
        if (!(request instanceof OffsetRequest)) {
            throw new IllegalStateException("The ProducerRequest is Invalid....");
        }
        OffsetRequest offsetRequest = (OffsetRequest)request;
        ListMultimap<String, OffsetRequest.PartitionOffsetBody> topicPartitionOffset = offsetRequest.getTopicPartitionOffset();

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        try {
            //apiKey
            dataOutputStream.writeShort(offsetRequest.getApiKey());
            //apiVersion
            dataOutputStream.writeShort(offsetRequest.getApiVersion());
            //correlationId
            dataOutputStream.writeInt(offsetRequest.getCorrelationId());
            //clientId
            CodeUtils.writeString(dataOutputStream, offsetRequest.getClientId());
            //replicaId
            dataOutputStream.writeInt(offsetRequest.getReplicaId());

            //topics
            Set<String> topics = topicPartitionOffset.keySet();
            dataOutputStream.writeInt(topics.size());
            for (String topic : topics) {
                //topicName
                CodeUtils.writeString(dataOutputStream, topic);
                List<OffsetRequest.PartitionOffsetBody> partitionOffsetBodies = topicPartitionOffset.get(topic);
                //partitionSize
                dataOutputStream.writeInt(partitionOffsetBodies.size());
                for (OffsetRequest.PartitionOffsetBody body : partitionOffsetBodies) {
                    //partition
                    dataOutputStream.writeInt(body.getPartitionId());
                    //time
                    dataOutputStream.writeLong(body.getTime());
                    //maxNumberOfOffsets
                    dataOutputStream.writeInt(body.getMaxNumberOfOffsets());
                }
            }
            dataOutputStream.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] tempOffsetRequestBytes = byteArrayOutputStream.toByteArray();
        ByteBuffer offsetRequestBytes = ByteBuffer.allocate(4 + tempOffsetRequestBytes.length);
        offsetRequestBytes.putInt(tempOffsetRequestBytes.length);
        offsetRequestBytes.put(tempOffsetRequestBytes);
        return offsetRequestBytes.array();
    }
}
