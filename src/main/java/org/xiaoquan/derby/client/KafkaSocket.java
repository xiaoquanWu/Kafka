package org.xiaoquan.derby.client;

import org.xiaoquan.derby.protocol.BaseResponse;
import org.xiaoquan.derby.protocol.Request;
import org.xiaoquan.derby.protocol.Response;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by XiaoQuan on 2015/1/5.
 */
public class KafkaSocket {

    private String ip;
    private int port;

    private boolean needResponse = true;

    public KafkaSocket(String ip, int port) {
        this.ip = ip;
        this.port = port;

    }
    public Response send(Request request) {
        Response response = null;
        Socket socket = null;
        OutputStream out = null;
        InputStream in = null;
        try {
            socket = new Socket(ip, port);
            socket.setSoTimeout(KafkaConfiguration.PRODUCER_TIMEOUT);
            out = socket.getOutputStream();
            in = socket.getInputStream();


            byte[] requestBytes = request.encodeRequest();

            System.out.println("Request Byte Size:" + requestBytes.length);
            out.write(requestBytes);
            out.flush();
            if (!needResponse) {
                return null;
            }
            response = new BaseResponse(in);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }


        return response;
    }

    public void setNeedResponse(boolean needResponse) {
        this.needResponse = needResponse;
    }

}
