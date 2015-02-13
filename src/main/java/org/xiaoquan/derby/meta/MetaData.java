package org.xiaoquan.derby.meta;

import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/12.
 */
public class MetaData {
    private Set<Broker> brokers = Sets.newHashSet();

    private ListMultimap<TopicMetadata, PartitionMetadata> topicPartitionMetadata = ArrayListMultimap.create();


    public Set<Broker> getBrokers() {
        return brokers;
    }

    public ListMultimap<TopicMetadata, PartitionMetadata> getTopicPartitionMetadata() {
        return topicPartitionMetadata;
    }

    public Set<Integer> getPartitionsByTopicName(String topicName) {
        Set<Integer> partitions = Sets.newHashSet();

        Set<TopicMetadata> topicMetadataSet = topicPartitionMetadata.keySet();
        for (TopicMetadata topicMetadata : topicMetadataSet) {
            if (topicMetadata.topicName.equals(topicName)) {
                for (PartitionMetadata partitionMetadata : topicPartitionMetadata.get(topicMetadata)) {
                    if (partitionMetadata.getPartitionErrorCode() == 0) {
                        partitions.add(partitionMetadata.getPartitionId());
                    }
                }
            }
        }
        return partitions;
    }


    /**
     * 假设只有一个broker
     * @return
     */
    public Broker getBroker() {
        return brokers.iterator().next();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("Brokers", brokers)
                .add("TopicMetadata", topicPartitionMetadata)
                .toString();
    }

    public static class Broker {
        private int nodeId;
        private String host;
        private int port;

        public Broker(int nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }

        public int getNodeId() {
            return nodeId;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return "{nodeId: " + nodeId + ", ip: " + host + ", port:" + port + "}";
        }
    }

    public static class TopicMetadata {
        private short topicErrorCode;
        private String topicName;

        public TopicMetadata(short topicErrorCode, String topicName) {
            this.topicErrorCode = topicErrorCode;
            this.topicName = topicName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TopicMetadata)) return false;

            TopicMetadata that = (TopicMetadata) o;

            if (topicName != null ? !topicName.equals(that.topicName) : that.topicName != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return topicName != null ? topicName.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "{topicErrorCode: " + topicErrorCode + ", topicName: " + topicName + "}";
        }
    }

    public static class PartitionMetadata {
        private short partitionErrorCode;
        private int partitionId;
        private int leader;
        private Set<Integer> replicas;
        private Set<Integer> isr;

        public short getPartitionErrorCode() {
            return partitionErrorCode;
        }

        public void setPartitionErrorCode(short partitionErrorCode) {
            this.partitionErrorCode = partitionErrorCode;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public void setPartitionId(int partitionId) {
            this.partitionId = partitionId;
        }

        public int getLeader() {
            return leader;
        }

        public void setLeader(int leader) {
            this.leader = leader;
        }

        public Set<Integer> getReplicas() {
            return replicas;
        }

        public void setReplicas(Set<Integer> replicas) {
            this.replicas = replicas;
        }

        public Set<Integer> getIsr() {
            return isr;
        }

        public void setIsr(Set<Integer> isr) {
            this.isr = isr;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PartitionMetadata)) return false;

            PartitionMetadata that = (PartitionMetadata) o;

            if (partitionId != that.partitionId) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "{partitionId:" + partitionId + ", partitionErrorCode:" + partitionErrorCode + ", leader:" + leader  + ", replicas:" + replicas + ", isr:" + isr + "}";
        }
    }

}
