package org.xiaoquan.derby.client;

import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xiaoquan.derby.codec.decoder.OffsetResponseDecoder;
import org.xiaoquan.derby.meta.MetaData;
import org.xiaoquan.derby.meta.MetaDataCache;
import org.xiaoquan.derby.protocol.OffsetRequest;
import org.xiaoquan.derby.protocol.OffsetResponse;
import org.xiaoquan.derby.protocol.Response;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by XiaoQuan on 2015/2/13.
 */
public class OffsetManagement {
    private static Logger logger = LoggerFactory.getLogger(OffsetManagement.class);

    private MetaData metaData;

    //topic, partition, offset
    private Table<String, Integer, Long> offsetTable = HashBasedTable.create();

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private OffsetManagement() {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                load();
                logger.info("Load offset success....");
            }
        }, 0, 60, TimeUnit.SECONDS);
    }


    private void load() {
        metaData = MetaDataCache.getInstance().get();
        HashMultimap<String, Integer> topicPartitions = metaData.getTopicPartitions();
        OffsetRequest offsetRequest = new OffsetRequest();
        Set<String> topics = topicPartitions.keySet();
        for (String topic : topics) {
            for (Integer partitionId : topicPartitions.get(topic)) {
                offsetRequest.add(topic, partitionId);
            }
        }
        MetaData.Broker broker = metaData.getBroker();
        KafkaSocket kafkaSocket = new KafkaSocket(broker.getHost(), broker.getPort());
        Response response = kafkaSocket.send(offsetRequest);
        response.setDecoder(new OffsetResponseDecoder());
        OffsetResponse offsetResponse = (OffsetResponse) response.decodeResponse();

        ListMultimap<String, OffsetResponse.PartitionOffsetStatus> topicPartitionOffsetResponse = offsetResponse.getTopicPartitionOffsetResponse();
        for (String topicName : topicPartitionOffsetResponse.keySet()) {
            for (OffsetResponse.PartitionOffsetStatus partitionOffsetStatus : topicPartitionOffsetResponse.get(topicName)) {
                offsetTable.put(topicName, partitionOffsetStatus.getPartitionId(), (Long) partitionOffsetStatus.getOffsets().toArray()[0]);
            }
        }
    }

    public Long getOffset(String topic, Integer partition) {
        return offsetTable.get(topic, partition);
    }

    public Table<String, Integer, Long> getOffsetTable() {
        return ImmutableTable.copyOf(this.offsetTable);
    }

    private static OffsetManagement offsetManagement = new OffsetManagement();
    public static OffsetManagement getInstance() {
        return offsetManagement;
    }

}
