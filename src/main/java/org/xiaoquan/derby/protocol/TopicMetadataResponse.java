package org.xiaoquan.derby.protocol;


import org.xiaoquan.derby.codec.decoder.Decoder;
import org.xiaoquan.derby.codec.decoder.TopicMetadataResponseDecoder;
import org.xiaoquan.derby.meta.MetaData;

/**
 * Created by XiaoQuan on 2015/1/7.
 */
public class TopicMetadataResponse extends BaseResponse {
    private MetaData metaData = new MetaData();



    public MetaData getMetaData() {
        return metaData;
    }
}
