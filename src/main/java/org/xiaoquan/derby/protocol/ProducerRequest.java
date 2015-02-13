package org.xiaoquan.derby.protocol;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.client.KafkaConfiguration;
import org.xiaoquan.derby.codec.encoder.Encoder;
import org.xiaoquan.derby.codec.encoder.ProducerRequestEncoder;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by XiaoQuan on 2015/1/14.
 */
public class ProducerRequest extends BaseRequest{

    private short requiredAcks;
    private int timeOut;

    private List<RequestBody> requestBodies = Lists.newArrayList();

    public ProducerRequest() {
        super();
        super.setApiKey(Request.PRODUCER_REQUEST);
        initConfiguration();
    }

    private void initConfiguration() {
        this.requiredAcks = KafkaConfiguration.PRODUCER_REQUIRED_ACK;
        this.timeOut = KafkaConfiguration.PRODUCER_TIMEOUT;
        super.setApiVersion(KafkaConfiguration.PRODUCER_API_VERSION);
        super.setCorrelationId(KafkaConfiguration.PRODUCER_CORRELATION_ID);
        super.setClientId(KafkaConfiguration.PRODUCER_CLIENT_ID);

    }

    /**
     * 指定要发送的主题和partition
     * @param topic
     * @param partitionId
     * @param record
     */
    public void addMessage(String topic, int partitionId, Record<String, String> record) {
        RequestBody requestBody = new RequestBody(topic);
        requestBody.getPartitionMessage().put(partitionId, record);
        this.requestBodies.add(requestBody);
    }

    @Override
    public Encoder createEncoder() {
        return new ProducerRequestEncoder();
    }

    public List<RequestBody> getRequestBodies() {
        return requestBodies;
    }


    //--------------------------------------------------------------------
    public class RequestBody {
        private String topic;
        //[partitionId:message];
        private Map<Integer,Record<String, String>> partitionMessage = Maps.newHashMap();

        public RequestBody(String topic) {
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }

        public Map<Integer, Record<String, String>> getPartitionMessage() {
            return partitionMessage;
        }
    }

    public short getRequiredAcks() {
        return requiredAcks;
    }

    public int getTimeOut() {
        return timeOut;
    }
}
