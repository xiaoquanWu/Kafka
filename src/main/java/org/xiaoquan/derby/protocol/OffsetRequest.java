package org.xiaoquan.derby.protocol;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.xiaoquan.derby.client.KafkaConfiguration;
import org.xiaoquan.derby.codec.encoder.Encoder;
import org.xiaoquan.derby.codec.encoder.OffsetRequestEncoder;

/**
 * Created by XiaoQuan on 2015/1/25.
 */
public class OffsetRequest extends BaseRequest {
    public static long TIME_TYPE_LATEST = -1;
    public static long TIME_TYPE_EARLIEST = -2;

    /**
     * The replica id indicates the node id of the replica initiating this request.
     * Normal client consumers should always specify this as -1 as they have no node id.
     * Other brokers set this to be their own node id.
     * The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes
     */
    private int replicaId;
    private ListMultimap<String, PartitionOffsetBody> topicPartitionOffset = ArrayListMultimap.create();



    public OffsetRequest() {
        super();
        super.setApiKey(Request.OFFSET_REQUEST);
        initConfiguration();
    }
    private void initConfiguration() {
        this.replicaId = KafkaConfiguration.OFFSET_REQUEST_REPLICA_ID;
        super.setCorrelationId(KafkaConfiguration.OFFSET_REQUEST_CORRELATION_ID);
        super.setApiVersion(KafkaConfiguration.OFFSET_REQUEST_API_VERSION);
        super.setClientId(KafkaConfiguration.OFFSET_REQUEST_CLIENT_ID);

    }

    /**
     *
     * @param topicName
     * @param partitionId
     * @param time  default is -1, means get latest offset
     * @param maxNumberOfOffsets default 1
     */
    public void add(String topicName, int partitionId, long time, int maxNumberOfOffsets) {
        topicPartitionOffset.put(topicName, new PartitionOffsetBody(partitionId, time, maxNumberOfOffsets));
    }

    public void add(String topicName, int partitionId, long time) {
        add(topicName, partitionId, time, KafkaConfiguration.OFFSET_REQUEST_DEFAULT_MAX_NUMBER_OF_OFFSETS);
    }

    public void add(String topicName, int partitionId) {
        add(topicName, partitionId, KafkaConfiguration.OFFSET_REQUEST_DEFAULT_TIME);
    }

    public int getReplicaId() {
        return replicaId;
    }

    public ListMultimap<String, PartitionOffsetBody> getTopicPartitionOffset() {
        return topicPartitionOffset;
    }

    @Override
    public Encoder createEncoder() {
        return new OffsetRequestEncoder();
    }

    public class PartitionOffsetBody {
        int partitionId;
        long time;
        int maxNumberOfOffsets;

        public PartitionOffsetBody(int partitionId, long time, int maxNumberOfOffsets) {
            this.partitionId = partitionId;
            this.time = time;
            this.maxNumberOfOffsets = maxNumberOfOffsets;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public long getTime() {
            return time;
        }

        public int getMaxNumberOfOffsets() {
            return maxNumberOfOffsets;
        }
    }

}
