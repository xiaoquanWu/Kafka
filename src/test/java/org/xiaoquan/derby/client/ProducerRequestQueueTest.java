package org.xiaoquan.derby.client;

import org.xiaoquan.derby.Record;
import org.xiaoquan.derby.protocol.ProducerRequest;

import java.util.concurrent.TimeUnit;

public class ProducerRequestQueueTest {
    public static void main(String[] args) throws Exception{
        final ProducerRequestQueue messageQueue = new ProducerRequestQueue(1024L, 1000L, 3);


        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 1024; i ++) {
                    System.out.println("add:" + i);
                    messageQueue.add(new Record<String, String>("" + i, "" + i));
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();


        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        ProducerRequest request = messageQueue.poll(5, TimeUnit.SECONDS);
                        if (request != null) {
                            System.out.println(request.getRequestBodies().size());
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();


        Thread.sleep(111111111111L);
    }

}