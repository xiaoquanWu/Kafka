package org.xiaoquan.derby.client;

import com.google.common.collect.Table;

import static org.junit.Assert.*;

public class OffsetManagementTest {

    public static void main(String[] args) throws Exception{
        OffsetManagement offsetManagement = OffsetManagement.getInstance();
        Thread.sleep(3000);
        Table<String, Integer, Long> offsetTable = offsetManagement.getOffsetTable();
        System.out.println(offsetTable);
        System.out.println(offsetTable.get("test02", 0));

        Thread.sleep(111111L);
    }

}