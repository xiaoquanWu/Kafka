package org.xiaoquan.derby.codec.encoder;

import org.xiaoquan.derby.codec.CodeUtils;
import org.xiaoquan.derby.protocol.Request;
import org.xiaoquan.derby.protocol.TopicMetadataRequest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/7.
 */
public class TopicMetadataRequestEncoder implements Encoder {
    @Override
    public byte[] encode(Request request) {
        if (!(request instanceof TopicMetadataRequest)) {
            throw new IllegalStateException("This request can't encode by the TopicMetadataRequestEncoder");
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            TopicMetadataRequest topicMetadataRequest = (TopicMetadataRequest) request;

            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            //apiKey
            dataOutputStream.writeShort(topicMetadataRequest.getApiKey());
            //apiVersion
            dataOutputStream.writeShort(topicMetadataRequest.getApiVersion());
            //correlationId
            dataOutputStream.writeInt(topicMetadataRequest.getCorrelationId());
            //ClientId
            CodeUtils.writeString(dataOutputStream, topicMetadataRequest.getClientId());

            //topics
            Set<String> topics = topicMetadataRequest.getTopicNames();
            dataOutputStream.writeInt(topics.size());
            for (String topic : topics) {
                CodeUtils.writeString(dataOutputStream, topic);
            }
            dataOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        byte[] requestBody = byteArrayOutputStream.toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + requestBody.length);
        byteBuffer.putInt(requestBody.length);
        byteBuffer.put(requestBody);
        return byteBuffer.array();
    }
}
