package org.xiaoquan.derby.protocol;


import com.google.common.base.Objects;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xiaoquan.derby.codec.decoder.Decoder;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by XiaoQuan on 2015/1/5.
 */
public class BaseResponse implements Response {
    private static final Logger logger = LoggerFactory.getLogger(BaseResponse.class);
    private int size;
    private int correlationId;
    private byte[] responseBytes;
    private Decoder decoder;

    protected BaseResponse() {
    }

    public BaseResponse(InputStream inputStream) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        int size = dataInputStream.readInt();
        byte[] responseBytes = new byte[size];
        dataInputStream.read(responseBytes);
        IOUtils.closeQuietly(inputStream);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + size);
        byteBuffer.putInt(size);
        byteBuffer.put(responseBytes);
        this.responseBytes = byteBuffer.array();
        logger.debug("ResponseSize:{}", this.responseBytes.length);
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    @Override
    public Response decodeResponse() throws IllegalStateException {
        return this.decoder.decode(this.getResponseBytes());
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    @Override
    public void setDecoder(Decoder decoder) {
        this.decoder = decoder;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("Size", this.getSize())
                .add("CorrelationId", this.getCorrelationId())
                .toString();
    }
}
