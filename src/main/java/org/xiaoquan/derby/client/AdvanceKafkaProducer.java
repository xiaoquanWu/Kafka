package org.xiaoquan.derby.client;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.meta.MetaData;
import org.xiaoquan.derby.meta.MetaDataCache;
import org.xiaoquan.derby.protocol.ProducerRequest;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by XiaoQuan on 2015/1/16.
 */
public class AdvanceKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(AdvanceKafkaProducer.class);

    private ProducerRequest producerRequest;
    private MetaData metaData = MetaDataCache.getInstance().get();

    private ListMultimap<String, Record<String, String>> messagesBuffer = ArrayListMultimap.create();


    private AtomicLong currentBytes = new AtomicLong();

    public void addMessage(String topicName, String message) {
        Record<String, String> record = new Record<String, String>(topicName, message);
        messagesBuffer.put(topicName, record);
    }




//    public boolean addMessage(String topicName, String message) {
//        Set<Integer> partitions = metaData.getPartitionsByTopicName(topicName);
//        if (partitions.isEmpty()) {
//            logger.warn("Topic{} is not found....", topicName);
//            return false;
//        }
//        //TODO
//        return true;
//    }


}
