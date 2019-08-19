package com.huawei.hwcloud.tarus.kvstore.service.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class LiteClient {

    public static final int SET_LEN = Constants.KEY_SIZE + Constants.VALUE_SIZE + 1;

    private final ByteBuffer setBuffer = ByteBuffer.allocateDirect(SET_LEN);

    private final long address = ((DirectBuffer) setBuffer).address();

    private final SocketChannel socketChannel;

    private final LRUCache lruCache;

    public LiteClient(String ip, int port, LRUCache lruCache) throws IOException {
        this.lruCache = lruCache;
        this.socketChannel = SocketChannel.open();
        this.socketChannel.configureBlocking(true);
        this.socketChannel.connect(new InetSocketAddress(ip, port));
    }

    public final boolean set(long key, byte[] value) throws IOException {
        setBuffer.clear();
        setBuffer.put(Constants.SET);
        setBuffer.putLong(key);
        Constants.UNSAFE.copyMemory(value, 16, null, address + 9, Constants.VALUE_SIZE);
        setBuffer.position(0).limit(SET_LEN);
        while (setBuffer.hasRemaining()) {
            socketChannel.write(setBuffer);
        }

        byteBuffer.clear();
        while (byteBuffer.hasRemaining()) {
            socketChannel.read(byteBuffer);
        }
        byteBuffer.flip();
        return byteBuffer.get() == Constants.SET_S;
    }

    private final ByteBuffer intBuffer = ByteBuffer.allocateDirect(Integer.BYTES);

    private final ByteBuffer longBuffer = ByteBuffer.allocateDirect(Long.BYTES);

    private final ByteBuffer getBuffer = ByteBuffer.allocateDirect(Integer.BYTES + 1);

    public final ByteBuffer get(int offset) {
        try {
            getBuffer.clear();
            getBuffer.put(Constants.GET);
            getBuffer.putInt(offset);
            getBuffer.flip();
            while (getBuffer.hasRemaining()) {
                socketChannel.write(getBuffer);
            }

            byteBuffer.clear();
            while (byteBuffer.hasRemaining()) {
                socketChannel.read(byteBuffer);
            }
            byteBuffer.flip();
            byte op = byteBuffer.get();


            ByteBuffer byteBuffer;
            int len = Constants.PAGE_SIZE;
            switch (op) {
                case Constants.GET_FULL:
                    byteBuffer = lruCache.getByteBuffer();
                    byteBuffer.position(0);
                    byteBuffer.limit(len);
                    while (byteBuffer.hasRemaining()) {
                        socketChannel.read(byteBuffer);
                    }
                    byteBuffer.flip();
                    return byteBuffer;
                case Constants.GET_HALF:
                    len = readInt();
                    if (len > 0) {
                        byteBuffer = lruCache.getByteBuffer();
                        byteBuffer.position(0);
                        byteBuffer.limit(len);
                        while (byteBuffer.hasRemaining()) {
                            socketChannel.read(byteBuffer);
                        }
                        byteBuffer.flip();
                        return byteBuffer;
                    }
                    return null;
                case Constants.GET_ZERO:
                    return null;
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);


    public final void meta(KVService kvService) throws IOException {
        byteBuffer.clear();
        byteBuffer.put(Constants.META);
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            socketChannel.write(byteBuffer);
        }
        int len = readInt();
        if (len > 0) {
            int pos = 0;
            int offset = 0;
            while (pos < len) {
                long key = readLong();
                offset++;
                kvService.put(key, offset);
                pos += Long.BYTES;
            }
        }
    }

    private final int readInt() throws IOException {
        intBuffer.clear();
        while (intBuffer.hasRemaining()) {
            socketChannel.read(intBuffer);
        }
        intBuffer.flip();
        return intBuffer.getInt();
    }

    private final long readLong() throws IOException {
        longBuffer.clear();
        while (longBuffer.hasRemaining()) {
            socketChannel.read(longBuffer);
        }
        longBuffer.flip();
        return longBuffer.getLong();
    }

    public final void close() {
        try {
            this.socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
