package org.xiaoquan.derby.client;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * 消费多个主题上的消息
 */
public class AdvanceKafkaConsumer {

    /**
     * 消费者在主题分区上当前消费的Offset
     */
    private Table<String, Integer, Long> topicPartitionConsumerOffset = HashBasedTable.create();
}
