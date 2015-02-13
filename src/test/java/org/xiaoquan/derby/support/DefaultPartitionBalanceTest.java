package org.xiaoquan.derby.support;

import org.junit.Test;
import org.xiaoquan.derby.meta.MetaDataCache;

import static org.junit.Assert.*;

public class DefaultPartitionBalanceTest {

    @Test
    public void testGetBalancePartition() throws Exception {
        DefaultPartitionBalance defaultPartitionBalance = DefaultPartitionBalance.getInstance();

        int index01 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");
        int index02 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");
        int index03 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");
        int index04 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");
        int index05 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");
        int index06 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");
        int index07 = defaultPartitionBalance.getBalancePartition(MetaDataCache.getInstance().get(), "test02");

        System.out.println(index01);
        System.out.println(index02);
        System.out.println(index03);
        System.out.println(index04);
        System.out.println(index05);
        System.out.println(index06);
        System.out.println(index07);

    }
}