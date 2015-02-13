package org.xiaoquan.derby.codec.decoder;

import com.google.common.collect.Sets;
import org.xiaoquan.derby.Constants;
import org.xiaoquan.derby.meta.MetaData;
import org.xiaoquan.derby.protocol.Response;
import org.xiaoquan.derby.protocol.TopicMetadataResponse;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/7.
 */
public class TopicMetadataResponseDecoder implements Decoder {
    @Override
    public Response decode(byte[] bytes) {
        TopicMetadataResponse topicMetadataResponse = new TopicMetadataResponse();

        MetaData metaData = topicMetadataResponse.getMetaData();
        try {
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            //size
            topicMetadataResponse.setSize(dataInputStream.readInt());
            //correlationId
            topicMetadataResponse.setCorrelationId(dataInputStream.readInt());

            //brokerSize
            int brokerSize = dataInputStream.readInt();
            for (int i = 0; i < brokerSize; i++) {
                //nodeId
                int nodeId = dataInputStream.readInt();
                short hostLength = dataInputStream.readShort();
                byte[] hostBytes = new byte[hostLength];
                dataInputStream.read(hostBytes, 0, hostLength);
                //host
                String host = new String(hostBytes, Constants.CHARACTER_SET);
                //port
                int port = dataInputStream.readInt();
                metaData.getBrokers().add(new MetaData.Broker(nodeId, host, port));

            }

            int topicMetadataSize = dataInputStream.readInt();
            for (int i = 0; i < topicMetadataSize; i++) {
                //topicErrorCode
                short topicErrorCode = dataInputStream.readShort();
                //topicName
                short topicNameLength = dataInputStream.readShort();
                byte[] topicNameBytes = new byte[topicNameLength];
                dataInputStream.read(topicNameBytes, 0, topicNameLength);
                String topicName = new String(topicNameBytes, Constants.CHARACTER_SET);

                MetaData.TopicMetadata topicMetadata = new MetaData.TopicMetadata(topicErrorCode, topicName);
                int partitionMetadataSize = dataInputStream.readInt();
                for (int j = 0; j < partitionMetadataSize; j++) {
                    //partitionErrorCode
                    short partitionErrorCode = dataInputStream.readShort();
                    //partitionId
                    int partitionId = dataInputStream.readInt();
                    //leader
                    int leader = dataInputStream.readInt();

                    int replicasSize = dataInputStream.readInt();
                    Set<Integer> replicas = Sets.newHashSet();
                    while (replicas.size() < replicasSize) {
                        replicas.add(dataInputStream.readInt());
                    }

                    int isrSize = dataInputStream.readInt();
                    Set<Integer> isr = Sets.newHashSet();
                    while (isr.size() < isrSize) {
                        isr.add(dataInputStream.readInt());
                    }

                    MetaData.PartitionMetadata partitionMetadata = new MetaData.PartitionMetadata();
                    partitionMetadata.setPartitionErrorCode(partitionErrorCode);
                    partitionMetadata.setPartitionId(partitionId);
                    partitionMetadata.setLeader(leader);
                    partitionMetadata.setReplicas(replicas);
                    partitionMetadata.setIsr(isr);

                    metaData.getTopicPartitionMetadata().put(topicMetadata, partitionMetadata);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return topicMetadataResponse;
    }
}
