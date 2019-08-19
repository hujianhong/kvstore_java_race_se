package com.huawei.hwcloud.tarus.kvstore.store.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import com.huawei.hwcloud.tarus.kvstore.util.Tools;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class IndexFile {
    public static final Unsafe UNSAFE = Tools.unsafe();

    private final FileChannel fileChannel;

    private final MappedByteBuffer buffer;

    private int count;

    private int writePos;

    private final long address;

    public IndexFile(String filePath) throws IOException {
        this.fileChannel = Channels.createFileChannel(filePath);
        int mapSize = Constants.TOTAL_SIZE * Constants.KEY_SIZE + Integer.BYTES;
        this.buffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
        this.address = ((DirectBuffer) this.buffer).address();
        this.count = UNSAFE.getInt(this.address);
        this.writePos += (4 + count * Long.BYTES);
    }

    public void writeLong(long value) {
        UNSAFE.putLong(address + this.writePos, value);
        this.writePos += Long.BYTES;
        count++;
        UNSAFE.putInt(address, count);
    }

    private int readPos;

    public void readReady() {
        this.readPos = Integer.BYTES;
    }

    public long readLong() {
        long value = UNSAFE.getLong(this.address + this.readPos);
        this.readPos += Long.BYTES;
        return value;
    }

    public int getCount() {
        return this.count;
    }

    public void flush() throws IOException {
    }
}
