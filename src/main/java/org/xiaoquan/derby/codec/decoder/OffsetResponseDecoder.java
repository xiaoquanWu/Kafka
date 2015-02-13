package org.xiaoquan.derby.codec.decoder;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import org.xiaoquan.derby.Constants;
import org.xiaoquan.derby.protocol.OffsetResponse;
import org.xiaoquan.derby.protocol.Response;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/25.
 */
public class OffsetResponseDecoder implements Decoder {
    @Override
    public Response decode(byte[] bytes) {
        OffsetResponse offsetResponse = new OffsetResponse();
        ListMultimap<String, OffsetResponse.PartitionOffsetStatus> topicPartitionOffsetResponse = offsetResponse.getTopicPartitionOffsetResponse();

        try {
            DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
            //size
            offsetResponse.setSize(dataInputStream.readInt());
            //correlationId
            offsetResponse.setCorrelationId(dataInputStream.readInt());

            //topicSize
            int topicSize = dataInputStream.readInt();
            for (int i = 0; i < topicSize; i++) {
                short topicNameLength = dataInputStream.readShort();
                byte[] topicNameBytes = new byte[topicNameLength];
                dataInputStream.read(topicNameBytes, 0, topicNameLength);
                String topicName = new String(topicNameBytes, Constants.CHARACTER_SET);

                int partitionOffsetSize = dataInputStream.readInt();
                for (int j = 0; j < partitionOffsetSize; j++) {
                    int partitionId = dataInputStream.readInt();
                    short errorCode = dataInputStream.readShort();

                    Set<Long> offsets = Sets.newHashSet();
                    int offsetSize = dataInputStream.readInt();
                    while (offsets.size() < offsetSize) {
                        offsets.add(dataInputStream.readLong());
                    }

                    OffsetResponse.PartitionOffsetStatus partitionOffsetStatus = new OffsetResponse.PartitionOffsetStatus(partitionId, errorCode, offsets);
                    topicPartitionOffsetResponse.put(topicName, partitionOffsetStatus);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return offsetResponse;
    }
}
