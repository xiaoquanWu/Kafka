package org.xiaoquan.derby.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private Logger logger = LoggerFactory.getLogger(KafkaSocket.class);

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

            logger.info("Request Byte Size:" + requestBytes.length);
            out.write(requestBytes);
            out.flush();
            if (!needResponse) {
                return null;
            }
//            Thread.sleep(1000);
            response = new BaseResponse(in);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null)
                    out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (socket != null)
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
