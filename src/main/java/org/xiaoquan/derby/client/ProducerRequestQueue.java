package org.xiaoquan.derby.client;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.meta.MetaDataCache;
import org.xiaoquan.derby.protocol.ProducerRequest;
import org.xiaoquan.derby.support.DefaultPartitionBalance;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by XiaoQuan on 2015/1/16.
 */
public class ProducerRequestQueue {

    private static final Logger logger = LoggerFactory.getLogger(ProducerRequestQueue.class);

    private BlockingQueue<ProducerRequestStatus> producerRequestQueue = new LinkedBlockingDeque<ProducerRequestStatus>();

    private long maxBufferBytes; //  == producerRequestQueue.size()与每个ProducerRequest的消息字节数的积
    private long minAvailableSize;
    private int maxAvailableTime;

    private DefaultPartitionBalance defaultPartitionBalance = DefaultPartitionBalance.getInstance();

    public ProducerRequestQueue(long maxBufferBytes, long minAvailableSize, int maxAvailableTime) {
        this.maxBufferBytes = maxBufferBytes;
        this.minAvailableSize = minAvailableSize;
        this.maxAvailableTime = maxAvailableTime;
    }

    public void add(Record<String, String> record) {
        ProducerRequestStatus producerRequestStatus = producerRequestQueue.peek();
        if (null == producerRequestStatus || producerRequestStatus.isAvailable()) {
            logger.debug("Create a new ProducerRequest");
            producerRequestStatus = new ProducerRequestStatus();
            producerRequestQueue.add(producerRequestStatus);
        }
        producerRequestStatus.addMessage(record);
        logger.debug("Add success, queue size: {}, {}", producerRequestQueue.size(), producerRequestStatus.currentBytes);
    }


    public ProducerRequest poll(int time, TimeUnit timeUnit) throws Exception {
        ProducerRequestStatus producerRequestStatus = producerRequestQueue.peek();
        if (producerRequestStatus == null) {
            timeUnit.sleep(time);
            producerRequestStatus = producerRequestQueue.peek();
        }
        if (producerRequestStatus != null && producerRequestStatus.isAvailable()) {
            ProducerRequestStatus requestStatus = producerRequestQueue.poll();
            logger.debug("Get producerRequest from queue, requestSize[{}]", requestStatus.currentBytes);
            return requestStatus.producerRequest;
        }
        return null;
    }

    private class ProducerRequestStatus {
        private DateTime createTime = DateTime.now();
        private AtomicLong currentBytes = new AtomicLong();
        private ProducerRequest producerRequest = new ProducerRequest();

        public Boolean addMessage(Record<String, String> record) {
            int messageLength = getRecordLength(record);
            if (currentBytes.addAndGet(messageLength) > (maxBufferBytes / producerRequestQueue.size())) {
                currentBytes.addAndGet(0 - messageLength);
                throw new IllegalStateException("Allocate [" + messageLength + "] bytes from buffer [" + currentBytes + "]/[" + maxBufferBytes + "] failed");
            }

            // partitionBalance
            Integer partitionId = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), record.getKey());
            if (partitionId == null) {
                logger.warn("The topic is not found: {}", record.getKey());
                return false;
            }
            producerRequest.addMessage(record.getKey(), partitionId, record);
            return true;
        }

        private int getRecordLength(Record<String, String> record) {
            return record.getValue().getBytes().length;
        }

        public boolean isAvailable() {
            DateTime end = DateTime.now();
            if (currentBytes.longValue() >= minAvailableSize || end.getSecondOfDay() - createTime.getSecondOfDay() >= maxAvailableTime) {
                return true;
            }
            return false;
        }

    }
}
