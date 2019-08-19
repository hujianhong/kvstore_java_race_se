package com.huawei.hwcloud.tarus.kvstore.service.example;

import com.huawei.hwcloud.tarus.kvstore.common.RPCUri;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TcpClient {

    private static final Logger log = LoggerFactory.getLogger(TcpClient.class);

    private static final int headlen = Integer.BYTES;

    private static final int buffsize = 2048;

    private static final int port = 9527;

    private static final int timeout = 60 * 1000;

    private static List<TcpClient> clients = new ArrayList<>();

    public static final TcpClient createConnect(final RPCUri uri) {
        TcpClient client = new TcpClient();
        client.setUri(uri);
        clients.add(client);
        return client;
    }

    public static void closeAll() {
        clients.forEach(c -> c.close());
        try {
            Thread.currentThread().sleep(1 * 1000);
        } catch (InterruptedException e) {
            throw new KVSException(KVSErrorCode.RPC_CLOSE_ERROR,
                    "close all tcp client error!");
        }
    }

    private RPCUri uri;

    private void setUri(final RPCUri uri){
        this.uri = uri;
    }

    public final byte[] send(byte[] buf) {

        Socket socket = new Socket();

        ByteArrayOutputStream baosHead = null;
        ByteArrayOutputStream baos = null;

        ByteBuffer headbuf = null;

        try {
            //create connect
            socket.connect(new InetSocketAddress(uri.getIp(), port));
            //send message
            ByteBuffer reqbuf = ByteBuffer.allocate(headlen + buf.length);
            reqbuf.putInt(buf.length).put(buf);
            log.info("begin to send message, request len=[{}]!", buf.length);

            OutputStream out = socket.getOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(out);
            writeMessage(bos, reqbuf.array());

            log.info("send message success!");

            socket.setSoTimeout(timeout);
            InputStream in = socket.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(in);

            log.info("begin to read message!");

            baosHead = new ByteArrayOutputStream();
            readPacket(bis, baosHead, Integer.BYTES);

            byte[] headMessage = baosHead.toByteArray();
            headbuf = ByteBuffer.allocate(headlen);
            headbuf.clear();
            headbuf.put(headMessage);
            headbuf.flip();
            int head = headbuf.getInt();
            baos = new ByteArrayOutputStream();
            readPacket(bis, baos, head);

            log.info("read message success!");

            byte[] res = baos.toByteArray();

            log.info("client res msg len is:[" + res.length + "]");
            return res;
        } catch (IOException e) {
            throw new KVSException(KVSErrorCode.RPC_READ_ERROR,
                    KVSErrorCode.RPC_READ_ERROR.getDescription(), e);
        }finally{
            if(socket != null){
                try {
                    socket.close();
                    socket = null;
                } catch (IOException e) {
                    log.warn("close error", e);
                }
            }
        }
    }

    public static void readPacket(BufferedInputStream bis, ByteArrayOutputStream baos, int len) throws IOException {

        int index = 0;

        int numberRead = 0;
        int tmpLen = len;

        byte[] buff = null;

        for (int i = 0; i < 20000; i++) {

            if (tmpLen < buffsize) {

                buff = new byte[tmpLen];
                numberRead = bis.read(buff, 0, tmpLen);
            } else {

                buff = new byte[buffsize];
                numberRead = bis.read(buff, 0, buffsize);
            }

            if (numberRead < 0) {

                break;
            } else if (numberRead < tmpLen) {

                index += numberRead;
                tmpLen = tmpLen - numberRead;

                byte[] tmpByte = new byte[numberRead];
                System.arraycopy(buff, 0, tmpByte, 0, numberRead);
                baos.write(tmpByte);
            } else if (numberRead == tmpLen) {
                index += numberRead;
                tmpLen = tmpLen - numberRead;
                baos.write(buff);
            } else {
                break;
            }

            if (tmpLen <= 0) {

                break;
            }
        }
    }

    public static void writeMessage(BufferedOutputStream bos, byte[] message) throws IOException {

        int len = message.length;
        int count = 0;
        int tmpLen = len;

        while (true) {
            if (tmpLen <= buffsize) {
                bos.write(message, count, tmpLen);
                bos.flush();
                count = count + tmpLen;
                tmpLen = 0;
            } else {
                bos.write(message, count, buffsize);
                bos.flush();
                count = count + buffsize;
                tmpLen = tmpLen - buffsize;
            }
            if (tmpLen == 0) {
                break;
            }
        }
    }

    public void close() {
        log.info("close tcp client!");
//        if(key != null && key.isValid()){
//            key.cancel();
//            key = null;
//        }
//        if(selector != null){
//            try {
//                SelectorFactory.closeSelector(selector);
//                selector = null;
//            } catch (IOException e) {
//                log.warn("close selector error", e);
//            }
//        }
//        if(socket != null){
//            try {
//                socket.close();
//                socket = null;
//            } catch (IOException e) {
//                log.warn("close error", e);
//            }
//        }
        clients.remove(this);
    }
}
