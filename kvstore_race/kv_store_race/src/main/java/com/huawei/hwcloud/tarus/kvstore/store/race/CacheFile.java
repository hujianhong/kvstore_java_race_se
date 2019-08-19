package com.huawei.hwcloud.tarus.kvstore.store.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class CacheFile {
    private final FileChannel fileChannel;

    protected final MappedByteBuffer buffer;

    private final int maxItem;

    protected int count;

    protected final long address;

    public CacheFile(String filePath, int maxItem) throws IOException {
        this.fileChannel = Channels.createFileChannel(filePath);
        this.buffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxItem * Constants.VALUE_SIZE);
        this.address =((DirectBuffer)this.buffer).address();
        this.maxItem = maxItem;
    }

    public void write(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            long address = ((DirectBuffer) buffer).address();
            Constants.UNSAFE.copyMemory(null, address, null, this.address + count * Constants.VALUE_SIZE, Constants.VALUE_SIZE);
        } else {
            Constants.UNSAFE.copyMemory(buffer.array(), 16, null, this.address + count * Constants.VALUE_SIZE, Constants.VALUE_SIZE);
        }
        this.count++;
    }

    public int getCount() {
        return count;
    }

    public boolean isFull() {
        return this.count == maxItem;
    }


    public int flush(FileChannel fileChannel, long position) throws IOException {
        int len = valueSize();
        this.buffer.position(0);
        this.buffer.limit(len);
        fileChannel.write(buffer, position);
        this.count = 0;
        return len;
    }

    public int valueSize() {
        return count * Constants.VALUE_SIZE;
    }

    public ByteBuffer view() {
        ByteBuffer dup = this.buffer.duplicate();
        dup.position(0);
        dup.limit(valueSize());
        return dup;
    }


    public void close() throws IOException {
        this.buffer.force();
        this.fileChannel.close();
    }

    public void reset(int count) {
        this.count = count;
    }
}
