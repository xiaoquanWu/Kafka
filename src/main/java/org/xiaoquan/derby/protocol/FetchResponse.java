package org.xiaoquan.derby.protocol;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Table;
import org.xiaoquan.derby.Record;

import java.util.Map;

/**
 * Created by XiaoQuan on 2015/1/31.
 */
public class FetchResponse extends BaseResponse {

    private ListMultimap<String, PartitionFetchResponse> topicPartitionFetchResponse = ArrayListMultimap.create();


    private Table<String, Integer, Record<String, String>> topicPartitionRecords = HashBasedTable.create();

    public ListMultimap<String, PartitionFetchResponse> getTopicPartitionFetchResponse() {
        return topicPartitionFetchResponse;
    }

    @Override
    public String toString() {
        return topicPartitionFetchResponse.toString();
    }

    public static class PartitionFetchResponse {
        private int partition;

        private short errorCode;
        private long highWaterMarkOffset;
        private int messageSetSize;

        private long offset;
        private int messageSize;

        private int crc;
        private byte magicByte;
        private byte attributes;

        private String key;
        private String value;

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public void setErrorCode(short errorCode) {
            this.errorCode = errorCode;
        }

        public void setHighWaterMarkOffset(long highWaterMarkOffset) {
            this.highWaterMarkOffset = highWaterMarkOffset;
        }

        public void setMessageSetSize(int messageSetSize) {
            this.messageSetSize = messageSetSize;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public void setMessageSize(int messageSize) {
            this.messageSize = messageSize;
        }

        public void setCrc(int crc) {
            this.crc = crc;
        }

        public void setMagicByte(byte magicByte) {
            this.magicByte = magicByte;
        }

        public void setAttributes(byte attributes) {
            this.attributes = attributes;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public short getErrorCode() {
            return errorCode;
        }

        public int getMessageSetSize() {
            return messageSetSize;
        }

        public long getHighWaterMarkOffset() {
            return highWaterMarkOffset;
        }

        public int getMessageSize() {
            return messageSize;
        }

        public int getCrc() {
            return crc;
        }

        public byte getMagicByte() {
            return magicByte;
        }

        public byte getAttributes() {
            return attributes;
        }

        @Override
        public String toString() {
            return "{" +
                    "partition=" + partition +
                    ", errorCode=" + errorCode +
                    ", highWaterMarkOffset=" + highWaterMarkOffset +
                    ", messageSetSize=" + messageSetSize +
                    ", offset=" + offset +
                    ", messageSize=" + messageSize +
                    ", crc=" + crc +
                    ", magicByte=" + magicByte +
                    ", attributes=" + attributes +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}
