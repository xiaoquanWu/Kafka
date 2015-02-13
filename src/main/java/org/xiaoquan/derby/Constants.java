package org.xiaoquan.derby;

/**
 * Created by XiaoQuan on 2015/1/5.
 */
public interface Constants {
    public static final String PRODUCER_MESSAGE_KEY = "DerbySoft";
    public static final short MESSAGE_COMPRESSION = 0;//Gzip

    /**
     * This is the offset used in kafka as the log sequence number. When the producer is sending messages it doesn't actually know the offset and can fill in any value here it likes.
     */
    public static final Long PRODUCER_MESSAGE_OFFSET = 0L;

    public static final String CHARACTER_SET = "UTF-8";

}
