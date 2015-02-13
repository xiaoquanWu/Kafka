package org.xiaoquan.derby.protocol;

import com.google.common.collect.Sets;
import org.xiaoquan.derby.client.KafkaConfiguration;
import org.xiaoquan.derby.codec.encoder.Encoder;
import org.xiaoquan.derby.codec.encoder.TopicMetadataRequestEncoder;

import java.util.Properties;
import java.util.Set;

/**
 * Created by XiaoQuan on 2015/1/7.
 */
public class TopicMetadataRequest extends BaseRequest {

    private Set<String> topicNames = Sets.newHashSet();

    public TopicMetadataRequest() {
        super();
        super.setApiKey(Request.METADATA_REQUEST);
        initConfiguration();
    }

    private void initConfiguration() {

        super.setCorrelationId(KafkaConfiguration.METADATA_CORRELATION_ID);
        super.setApiVersion(KafkaConfiguration.METADATA_API_VERSION);
        super.setClientId(KafkaConfiguration.METADATA_CLIENT_ID);
    }

    public void addTopic(String topicName) {
        this.topicNames.add(topicName);
    }

    @Override
    public Encoder createEncoder() {
        return new TopicMetadataRequestEncoder();
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }
}
