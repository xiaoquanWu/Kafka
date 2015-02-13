package org.xiaoquan.derby.protocol;


import org.xiaoquan.derby.codec.decoder.Decoder;

/**
 * Created by XiaoQuan on 2015/1/5.
 */
public interface Response {
    public Response decodeResponse();
    public void setDecoder(Decoder decoder);
}
