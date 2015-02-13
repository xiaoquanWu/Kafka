package org.xiaoquan.derby.codec.decoder;

import org.xiaoquan.derby.protocol.Response;

/**
 * Created by XiaoQuan on 2015/1/13.
 */
public interface Decoder {
    public Response decode(byte[] bytes);
}
