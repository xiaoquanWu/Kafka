package org.xiaoquan.derby.support;

import com.google.common.collect.Maps;
import org.xiaoquan.derby.meta.MetaData;
import org.xiaoquan.derby.meta.MetaDataCache;

import java.util.Map;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/16.
 */
public class DefaultPartitionBalance {

    private Map<String, Integer> topicPartitionIndex = Maps.newConcurrentMap();

    private DefaultPartitionBalance(){}
    private static DefaultPartitionBalance defaultPartitionBalance = new DefaultPartitionBalance();

    public static DefaultPartitionBalance getInstance() {
        return  defaultPartitionBalance;
    }

    public Integer getBalancePartition(MetaData metaData, String topicName) {
        Set<Integer> partitions = metaData.getPartitionsByTopicName(topicName);
        if (partitions == null || partitions.isEmpty()) {
            return null;
        }

        if ( !topicPartitionIndex.containsKey(topicName)) {
            topicPartitionIndex.put(topicName, 0);
            return 0;
        }



        Integer index = topicPartitionIndex.get(topicName);
        index = (index+1) % partitions.size();
        topicPartitionIndex.put(topicName, index);
        return topicPartitionIndex.get(topicName);
    }
}
