package com.huawei.hwcloud.tarus.kvstore.store.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public final class LiteHandler implements Runnable {

    private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(Constants.KEY_SIZE);

    private final ByteBuffer valueBuffer = ByteBuffer.allocateDirect(Constants.VALUE_SIZE);

    private final ByteBuffer intBuffer = ByteBuffer.allocateDirect(Integer.BYTES);

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);

    private final SocketChannel socketChannel;

    private final Bucket bucket;

    public LiteHandler(Bucket bucket, SocketChannel socketChannel) {
        this.bucket = bucket;
        this.socketChannel = socketChannel;
    }

    @Override
    public void run() {
        while (true) {
            try {
                doRun();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void doRun() throws IOException {
        this.byteBuffer.clear();
        while (byteBuffer.hasRemaining()) {
            socketChannel.read(byteBuffer);
        }
        byteBuffer.flip();
        int op = byteBuffer.get();
        switch (op) {
            case Constants.SET:
                handleSet(socketChannel);
                break;
            case Constants.GET:
                handleGet(socketChannel);
                break;
            case Constants.META:
                handleMeta(socketChannel);
                break;
            default:
                System.out.println("not supported op:" + op);
        }
    }


    private void handleSet(SocketChannel socketChannel) throws IOException {
        keyBuffer.clear();
        while (keyBuffer.hasRemaining()) {
            socketChannel.read(keyBuffer);
        }
        keyBuffer.flip();
        long key = keyBuffer.getLong();
        valueBuffer.clear();
        while (valueBuffer.hasRemaining()) {
            socketChannel.read(valueBuffer);
        }
        bucket.set(key, valueBuffer);
        writeByte(socketChannel, Constants.SET_S);
    }

    private void writeInt(SocketChannel socketChannel, int value) throws IOException {
        intBuffer.clear();
        intBuffer.putInt(value);
        intBuffer.flip();
        while (intBuffer.hasRemaining()) {
            socketChannel.write(intBuffer);
        }
    }

    private void writeByte(SocketChannel socketChannel, byte value) throws IOException {
        byteBuffer.clear();
        byteBuffer.put(value);
        byteBuffer.flip();
        while (byteBuffer.hasRemaining()) {
            socketChannel.write(byteBuffer);
        }
    }

    private int readInt(SocketChannel socketChannel) throws IOException {
        intBuffer.clear();
        while (intBuffer.hasRemaining()) {
            socketChannel.read(intBuffer);
        }
        intBuffer.flip();
        return intBuffer.getInt();
    }

    private void handleGet(SocketChannel socketChannel) throws IOException {
        int offset = readInt(socketChannel);
        ByteBuffer buffer = bucket.page(offset);
        if (buffer != null) {
            if (buffer.limit() == Constants.PAGE_SIZE) {
                writeByte(socketChannel, Constants.GET_FULL);
            } else {
                writeByte(socketChannel, Constants.GET_HALF);
                writeInt(socketChannel, buffer.limit());
            }
            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        } else {
            writeByte(socketChannel, Constants.GET_ZERO);
        }
    }

    private void handleMeta(SocketChannel socketChannel) throws IOException {
        IndexFile indexFile = bucket.getIndexFile();
        if (indexFile.getCount() > 0) {
            int size = indexFile.getCount() * Long.BYTES;
            writeInt(socketChannel, size);
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(size);
            indexFile.readReady();
            for (int i = 0, count = indexFile.getCount(); i < count; i++) {
                byteBuffer.putLong(indexFile.readLong());
            }
            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                socketChannel.write(byteBuffer);
            }
            ((DirectBuffer) byteBuffer).cleaner().clean();
        } else {
            writeInt(socketChannel, 0);
        }
    }
}
