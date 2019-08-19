package com.huawei.hwcloud.tarus.kvstore.store.example;

import com.huawei.hwcloud.tarus.kvstore.common.RPCUri;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TcpServer extends Thread{

    private static final Logger log = LoggerFactory.getLogger(TcpServer.class);

    private static int timeout = 60 * 1000;

    private static final int port = 9527;

    private static TcpServer nioserver = null;

    public static void init(final RpcProcess rpcProcess) {
        nioserver = new TcpServer(rpcProcess);
        nioserver.start();
    }

    public TcpServer(RpcProcess rpcProcess) {
        this.rpcProcess = rpcProcess;
    }

    private RpcProcess rpcProcess;

    public void run() {
        ServerSocket server = null;

        try {
            server = new ServerSocket(port);
        } catch (IOException e) {
            throw new KVSException(KVSErrorCode.RPC_LISTEN_ERROR,
                    KVSErrorCode.RPC_LISTEN_ERROR.getDescription(), e);
        }

        while (true) {
            Socket socekt = null;
            try {
                socekt = server.accept();
                if (socekt != null) {
                    log.info("recieve a connect...");
                    BlockNIOHandle blockNIOHandle = new BlockNIOHandle(socekt, rpcProcess);
                    blockNIOHandle.start();
                }
            } catch (IOException e) {
                log.warn("socket server error", e);
            }
        }
    }

    private RPCUri uri;


    public void stopAll() {

    }
}
