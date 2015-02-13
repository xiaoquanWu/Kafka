package org.xiaoquan.derby.protocol;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.lang.StringUtils;
import org.xiaoquan.derby.client.KafkaConfiguration;
import org.xiaoquan.derby.codec.encoder.Encoder;
import org.xiaoquan.derby.codec.encoder.FetchRequestEncoder;

import java.util.Properties;

/**
 * Created by XiaoQuan on 2015/1/31.
 */
public class FetchRequest extends BaseRequest{

    /**
     * The replica id indicates the node id of the replica initiating this request.
     * Normal client consumers should always specify this as -1 as they have no node id.
     * Other brokers set this to be their own node id.
     * The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
     */
    private int replicaId;

    /**
     * The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
     */
    private int maxWaitTime;

    private int minBytes;

    private ListMultimap<String, RequestBody> topicPartitionFetchMap = ArrayListMultimap.create();

    public FetchRequest() {
        super();
        super.setApiKey(Request.FETCH_REQUEST);
        initConfiguration();
    }

    private void initConfiguration() {
        this.replicaId = KafkaConfiguration.FETCH_REQUEST_REPLICA_ID;
        this.maxWaitTime = KafkaConfiguration.FETCH_REQUEST_MAX_WAIT_TIME;
        this.minBytes = KafkaConfiguration.FETCH_REQUEST_MIN_BYTES;
        super.setCorrelationId(KafkaConfiguration.FETCH_REQUEST_CORRELATION_ID);
        super.setApiVersion(KafkaConfiguration.FETCH_REQUEST_API_VERSION);
        super.setClientId(KafkaConfiguration.FETCH_REQUEST_CLIENT_ID);
    }


    public void add(String topic, int partitionId, long fetchOffset, int maxBytes) {
        if (StringUtils.isBlank(topic)) throw new IllegalStateException("topic is required");
        topicPartitionFetchMap.put(topic, new RequestBody(partitionId, fetchOffset, maxBytes));
    }

    public void add(String topic, int partitionId, long fetchOffset) {
        add(topic, partitionId, fetchOffset, KafkaConfiguration.FETCH_REQUEST_MAX_BYTES);
    }

    @Override
    public Encoder createEncoder() {
        return new FetchRequestEncoder();
    }

    public int getReplicaId() {
        return replicaId;
    }

    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    public int getMinBytes() {
        return minBytes;
    }

    public void setMaxWaitTime(int maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    public void setMinBytes(int minBytes) {
        this.minBytes = minBytes;
    }

    public ListMultimap<String, RequestBody> getTopicPartitionFetchMap() {
        return topicPartitionFetchMap;
    }

    //---------------------------------------
    public class RequestBody {
        private int partitionId;
        private long fetchOffset;
        private int maxBytes;

        public RequestBody(int partitionId, long fetchOffset, int maxBytes) {
            this.partitionId = partitionId;
            this.fetchOffset = fetchOffset;
            this.maxBytes = maxBytes;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public long getFetchOffset() {
            return fetchOffset;
        }

        public int getMaxBytes() {
            return maxBytes;
        }
    }
}
