package org.xiaoquan.derby.protocol;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/25.
 */
public class OffsetResponse extends BaseResponse{

    private ListMultimap<String, PartitionOffsetStatus> topicPartitionOffsetResponse = ArrayListMultimap.create();


    public ListMultimap<String, PartitionOffsetStatus> getTopicPartitionOffsetResponse() {
        return topicPartitionOffsetResponse;
    }

    @Override
    public String toString() {
        return topicPartitionOffsetResponse.toString();
    }

    public static class PartitionOffsetStatus {
        private int partitionId;
        private short errorCode;
        private Set<Long> offsets ;

        public PartitionOffsetStatus(int partitionId, short errorCode, Set<Long> offsets) {
            this.partitionId = partitionId;
            this.errorCode = errorCode;
            this.offsets = offsets;
        }

        @Override
        public String toString() {
            return "{partitionId:" + partitionId + ", errorCode: " + errorCode + ", offSets:" + offsets + "}";
        }
    }
}
