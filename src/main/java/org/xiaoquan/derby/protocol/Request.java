package org.xiaoquan.derby.protocol;

import org.xiaoquan.derby.codec.encoder.Encoder;

/**
 * Created by XiaoQuan on 2015/1/5.
 */
public interface Request {
    public static final short PRODUCER_REQUEST = 0;
    public static final short FETCH_REQUEST = 1;
    public static final short OFFSET_REQUEST = 2;
    public static final short METADATA_REQUEST = 3;
    public static final short OFFSET_COMMIT_REQUEST = 8;
    public static final short OFFSET_FETCH_REQUEST = 9;
    public static final short CONSUMER_METADATA_REQUEST = 10;


    public byte[] encodeRequest() throws Exception;

}
