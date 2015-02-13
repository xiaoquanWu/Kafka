package org.xiaoquan.derby.protocol;

/**
 * Created by XiaoQuan on 2015/1/5.
 */


import org.xiaoquan.derby.codec.encoder.Encoder;

/**
 * ApiKey(int16): This is a numeric id for the API being invoked (i.e. is it a metadata request, a produce request, a fetch request, etc).
 * ApiVersion(int16) : This is a numeric version number for this api. We version each API and this version number allows the server to properly
 * interpret the request as the protocol evolves. Responses will always be in the format corresponding to the request version. Currently the supported version for all APIs is 0.
 * CorrelationId(int32): This is a user-supplied integer. It will be passed back in the response by the server, unmodified. It is useful for matching request and response between the client and server.
 * ClientId(string): This is a user supplied identifier for the client application. The user can use any identifier they like and it will be used when logging errors, monitoring aggregates, etc.
 * For example, one might want to monitor not just the requests per second overall, but the number coming from each client application (each of which could reside on multiple servers). This id acts as a logical grouping across all requests from a particular client.
 */
public abstract class BaseRequest implements Request {
    private int size;
    private short apiKey;
    private short apiVersion;
    private int correlationId;
    private String clientId;

    private Encoder encoder;

    public BaseRequest() {
        this.encoder = createEncoder();
    }


    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public short getApiKey() {
        return apiKey;
    }

    protected void setApiKey(short apiKey) {
        this.apiKey = apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(short apiVersion) {
        this.apiVersion = apiVersion;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(int correlationId) {
        this.correlationId = correlationId;
    }

    public abstract Encoder createEncoder();
    @Override
    public byte[] encodeRequest() throws Exception{
        return this.encoder.encode(this);
    }
}
