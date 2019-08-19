package com.huawei.hwcloud.tarus.kvstore.common;

import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.net.URI;
import java.net.URISyntaxException;

public class RPCUri {

    private String uri;
    private String protocol;
    private String ip;
    private int port;

    public RPCUri(final String uri){
        setUri(uri);
        parseUri();
    }

    public final String getUri() {
        return uri;
    }

    public final String getProtocol() {
        return protocol;
    }

    public final String getIp() {
        return ip;
    }

    public final int getPort() {
        return port;
    }

    private final void setUri(final String uri) {
        this.uri = uri;
        Validate.isTrue(StringUtils.isNotEmpty(this.uri), "empty uri=[" + this.uri + "]");
    }

    private final void setProtocol(final String protocol) {
        this.protocol = protocol;
    }

    private final void setIp(final String ip) {
        this.ip = ip;
    }

    private final void setPort(final int port) {
        this.port = port;
    }

    private final void parseUri(){
        try {
            URI tmpUri = new URI(getUri());
            setProtocol(tmpUri.getScheme());
            setIp(tmpUri.getHost());
            setPort(tmpUri.getPort());
        } catch (URISyntaxException e) {
            throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR,
                    String.format("uri format:[%s] error!", getUri()));
        }
    }

    public static void main(String [] args){
        String uriStr = "tcp://127.0.0.1";
        RPCUri uri = new RPCUri(uriStr);
        System.out.println(uri.getUri());
        System.out.println(uri.getProtocol());
        System.out.println(uri.getIp());
        System.out.println(uri.getPort());
    }
}
