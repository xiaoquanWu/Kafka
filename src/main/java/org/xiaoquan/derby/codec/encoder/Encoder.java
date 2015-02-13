package org.xiaoquan.derby.codec.encoder;

import org.xiaoquan.derby.protocol.Request;

/**
 * Created by XiaoQuan on 2015/1/13.
 */
public interface Encoder {
    public byte[] encode(Request request);

}
