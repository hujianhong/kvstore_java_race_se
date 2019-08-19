package com.huawei.hwcloud.tarus.kvstore.store.race;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class LiteServer {

    private final int port;

    private final ServerSocketChannel serverSocketChannel;

    private final Bucket bucket;

    public LiteServer(Bucket bucket, int port) throws IOException {
        this.bucket = bucket;
        this.port = port;
        this.serverSocketChannel = ServerSocketChannel.open();
        this.serverSocketChannel.bind(new InetSocketAddress(port));
        this.serverSocketChannel.configureBlocking(true);
    }


    public void service() {
        while (true) {
            try {
                SocketChannel socketChannel = serverSocketChannel.accept();
                new Thread(new LiteHandler(bucket, socketChannel)).start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
