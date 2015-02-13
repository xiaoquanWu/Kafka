package org.xiaoquan.derby.meta;

import org.xiaoquan.derby.client.KafkaConfiguration;

import java.util.Properties;

import static org.junit.Assert.*;

public class MetaDataCacheTest {

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        KafkaConfiguration.KAFKA_BROKER = "127.0.0.1:9095";
        MetaData metaData = MetaDataCache.getInstance().get();

        System.out.println(metaData);
//        Thread.sleep(60000);
    }

}