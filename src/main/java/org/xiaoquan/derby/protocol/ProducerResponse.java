package org.xiaoquan.derby.protocol;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Created by XiaoQuan on 2015/1/14.
 */
public class ProducerResponse extends BaseResponse{

    private List<ResponseBody> responseBodies = Lists.newArrayList();

    public List<ResponseBody> getResponseBodies() {
        return responseBodies;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("ProducerResponse", responseBodies)
                .toString();
    }

    //------------------------------
    public static class ResponseBody {
        private String topic;
        private Map<Integer, PartitionStatus> partitionStatusMap = Maps.newHashMap();

        public ResponseBody(String topic) {
            this.topic = topic;
        }
        public void addPartitionStatus(int partitionId, short errorCode, long offSet) {
            partitionStatusMap.put(partitionId, new ProducerResponse.PartitionStatus(partitionId, errorCode, offSet));
        }

        @Override
        public String toString() {
            return "{topic: " + topic + ", partitionStatusMap: " + partitionStatusMap + "}";
        }
    }

    public static class PartitionStatus {
        private int partitionId;
        private short errorCode;
        private long offSet;

        public PartitionStatus(int partitionId, short errorCode, Long offSet) {
            this.partitionId = partitionId;
            this.errorCode = errorCode;
            this.offSet = offSet;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public short getErrorCode() {
            return errorCode;
        }

        public long getOffSet() {
            return offSet;
        }

        @Override
        public String toString() {
            return "{partitionId:" + partitionId + ", errorCode: " + errorCode + ", offSet:" + offSet + "}";
        }
    }
}
