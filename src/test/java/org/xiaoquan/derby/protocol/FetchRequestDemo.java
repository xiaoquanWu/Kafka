package org.xiaoquan.derby.protocol;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by XiaoQuan on 2015/1/4.
 */

public class FetchRequestDemo {
    static String topicName = "test03";
//    String topicName = "perf_carlsonplugin_raw";

    String clientId = "Fetch-DerbySoft-XQ";

    static FetchRequest fetchRequest = new FetchRequest();

    static {
        fetchRequest.add(topicName, 0, 73);
    }


    public byte[] encodeFetchRequest() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        //replicaId
        dataOutputStream.writeInt(fetchRequest.getReplicaId());
        //maxWaitTime
        dataOutputStream.writeInt(fetchRequest.getMaxWaitTime());
        //minBytes
        dataOutputStream.writeInt(fetchRequest.getMinBytes());
        //
        dataOutputStream.writeInt(fetchRequest.getTopicPartitionFetchMap().keySet().size());
        //topicName
        dataOutputStream.writeShort(this.topicName.getBytes().length);
        dataOutputStream.write(this.topicName.getBytes());
        //

        List<FetchRequest.RequestBody> requestBodies = fetchRequest.getTopicPartitionFetchMap().get(topicName);
        dataOutputStream.writeInt(requestBodies.size());

        for (FetchRequest.RequestBody b : requestBodies) {
            //partition
            dataOutputStream.writeInt(b.getPartitionId());
            //fetchOffset
            dataOutputStream.writeLong(b.getFetchOffset());
            //maxBytes
            System.out.println("--------------------------------->:" + b.getMaxBytes());
            dataOutputStream.writeInt(b.getMaxBytes());
//            dataOutputStream.writeInt(38);
        }

        dataOutputStream.close();
        return byteArrayOutputStream.toByteArray();

    }


    public byte[] encodeRequest() throws Exception {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        //apiKey
        dataOutputStream.writeShort(Request.FETCH_REQUEST);
        //apiVersion
        dataOutputStream.writeShort(fetchRequest.getApiVersion());
        //correlationId
        dataOutputStream.writeInt(fetchRequest.getCorrelationId());
        //clientId
        dataOutputStream.writeShort(fetchRequest.getClientId().getBytes().length);
        dataOutputStream.write(fetchRequest.getClientId().getBytes());

        //requestMessage
        byte[] fetchRequest = encodeFetchRequest();
        dataOutputStream.write(fetchRequest);
        dataOutputStream.close();

        byte[] temRequest = byteArrayOutputStream.toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + temRequest.length);
        byteBuffer.putInt(temRequest.length);
        byteBuffer.put(temRequest);
        return byteBuffer.array();
    }

    public static void main(String[] args) {


        Socket socket = null;
        OutputStream out = null;
        InputStream in = null;
        try {
            socket = new Socket("127.0.0.1", 9093);
//            socket = new Socket("bdprodk01.dbhotelcloud.com", 9092);
            System.out.println(socket.isConnected());

            out = socket.getOutputStream();
            in = socket.getInputStream();

            byte[] encodeRequest = new FetchRequestDemo().encodeRequest();
            System.out.println("RQ:" + encodeRequest.length);
            out.write(encodeRequest);
            out.flush();

            DataInputStream dataInputStream = new DataInputStream(in);
            System.out.println("ResponseSize: " + (Integer.parseInt(dataInputStream.readInt()+"") + 4));
            System.out.println("correlationId:" + dataInputStream.readInt());

            int fetchResponseSize = dataInputStream.readInt();
            for (int i = 0; i < fetchResponseSize; i++) {
                System.out.println("{");
                short topicNameLength = dataInputStream.readShort();
                byte[] topicName = new byte[topicNameLength];
                dataInputStream.read(topicName, 0, topicNameLength);
                System.out.println("topicName : " + new String(topicName));

                int size = dataInputStream.readInt();
                for (int j = 0; j < size; j++) {
                    System.out.println("***********************");
                    System.out.println("partition:" + dataInputStream.readInt());
                    System.out.println("errorCode:" + dataInputStream.readShort());
                    System.out.println("highWaterMarkOffset:" + dataInputStream.readLong());
                    int messageSetByteSize = dataInputStream.readInt();
                    System.out.println("messageSetByteSize:" + messageSetByteSize);
                    if (messageSetByteSize == 0) break;

//                    int messageSize = dataInputStream.readInt();
//                    for (int jj = 0; jj < messageSize; jj++) {
                    System.out.println("----------------");
                    System.out.println("offset: " + dataInputStream.readLong());
                    System.out.println("messageSize: " + dataInputStream.readInt());
                    System.out.println("crc: " + dataInputStream.readInt());
                    System.out.println("magicByte: " + dataInputStream.readByte());
                    System.out.println("attribute: " + dataInputStream.readByte());

                    //
                    int messageKeyLength = dataInputStream.readInt();
                    byte[] key = new byte[messageKeyLength];
                    dataInputStream.read(key);
                    System.out.println("Key: " + new String(key));

                    int messageValueLength = dataInputStream.readInt();
                    byte[] value = new byte[messageValueLength];
                    dataInputStream.read(value);
                    System.out.println("Value: " + new String(value));
//                    }
                }
                System.out.println("}");
            }

            out.close();
            in.close();
            socket.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (socket != null) socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (out != null) out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (in != null) in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
