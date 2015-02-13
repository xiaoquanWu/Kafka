package org.xiaoquan.derby.meta;

import com.google.common.base.Splitter;
import com.google.common.cache.*;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xiaoquan.derby.client.KafkaConfiguration;
import org.xiaoquan.derby.client.KafkaSocket;
import org.xiaoquan.derby.codec.decoder.TopicMetadataResponseDecoder;
import org.xiaoquan.derby.protocol.Response;
import org.xiaoquan.derby.protocol.TopicMetadataRequest;
import org.xiaoquan.derby.protocol.TopicMetadataResponse;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by XiaoQuan on 2015/1/14.
 */
public class MetaDataCache {
    private static final Logger logger = LoggerFactory.getLogger(MetaDataCache.class);

    KafkaSocket kafkaSocket;

    private LoadingCache<Set<String>, MetaData> metaCache = CacheBuilder.newBuilder()
            .expireAfterAccess(20L, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<Set<String>, MetaData>() {
                @Override
                public void onRemoval(RemovalNotification<Set<String>, MetaData> removalNotification) {
                    logger.info("remove: " + removalNotification.getKey());
                }
            })
            .refreshAfterWrite(20L, TimeUnit.MINUTES)
            .build(new CacheLoader<Set<String>, MetaData>() {
                @Override
                public MetaData load(Set<String> topics) throws Exception {
                    logger.info("load metadata.....");
                    return loadMetadata(topics);
                }
            });

    private MetaDataCache() {
        String[] hostPort = KafkaConfiguration.KAFKA_BROKER.split(":");
        kafkaSocket = new KafkaSocket(hostPort[0], Integer.parseInt(hostPort[1]));
    }
    private static MetaDataCache metaDataCache = new MetaDataCache();

    public static MetaDataCache getInstance() {
        return metaDataCache;
    }

    public MetaData get() {
        try {
            Set<String> topics = Sets.newHashSet(Splitter.on(",").omitEmptyStrings().trimResults().split(KafkaConfiguration.KAFKA_TOPIC_NAMES));
            return metaCache.get(topics);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    private MetaData loadMetadata(Set<String> topics) {
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest();
        topicMetadataRequest.getTopicNames().addAll(topics);
        Response response = kafkaSocket.send(topicMetadataRequest);
        response.setDecoder(new TopicMetadataResponseDecoder());
        TopicMetadataResponse topicMetadataResponse = (TopicMetadataResponse)response.decodeResponse();
        return topicMetadataResponse.getMetaData();
    }

}
