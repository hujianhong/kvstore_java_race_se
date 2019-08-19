package com.huawei.hwcloud.tarus.kvstore.store.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import moe.cnkirito.kdio.DirectIOLib;
import moe.cnkirito.kdio.DirectIOUtils;
import moe.cnkirito.kdio.DirectRandomAccessFile;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public final class DataFile {
    public static final DirectIOLib DIRECT_IO_LIB = DirectIOLib.getLibForPath("/");

    private FileChannel fileChannel;

    private final String fileName;

    private final CacheFile cacheFile;

    private long writePosition;

    private ByteBuffer cacheView;

    private long pageNum = -1;

    private final ByteBuffer readByteBuffer;

    private long readAdress;

    private DirectRandomAccessFile directRandomAccessFile;

    private final boolean dioSupported;

    public DataFile(String fileName) throws IOException {
        this(fileName, fileName + ".cache");
    }

    public DataFile(String fileName, String cacheFileName) throws IOException {
        this.fileName = fileName;
        if (DirectIOLib.binit) {

            this.directRandomAccessFile = new DirectRandomAccessFile(new File(this.fileName), "rw");
            this.readByteBuffer = DirectIOUtils.allocateForDirectIO(DIRECT_IO_LIB, Constants.PAGE_SIZE);
            this.readAdress = ((DirectBuffer) this.readByteBuffer).address();
            this.writePosition = this.directRandomAccessFile.length();
            this.dioSupported = true;
        } else {

            this.fileChannel = Channels.createFileChannel(this.fileName);
            this.writePosition = this.fileChannel.size();
            this.readByteBuffer = ByteBuffer.allocateDirect(Constants.PAGE_SIZE);
            this.dioSupported = false;
        }
        this.cacheFile = new CacheFile(cacheFileName, Constants.PAGE_SIZE / Constants.VALUE_SIZE);
    }

    public void write(ByteBuffer buffer) throws IOException {
        if (this.cacheFile.isFull()) {
            if (dioSupported) {
                Constants.UNSAFE.copyMemory(null, cacheFile.address, null, readAdress, Constants.PAGE_SIZE);
                readByteBuffer.position(0);
                readByteBuffer.limit(Constants.PAGE_SIZE);
                directRandomAccessFile.write(readByteBuffer, this.writePosition);
                cacheFile.count = 0;
                this.writePosition += Constants.PAGE_SIZE;
            } else {
                this.writePosition += this.cacheFile.flush(this.fileChannel, this.writePosition);
            }
        }
        this.cacheFile.write(buffer);
        this.cacheView = null;
    }



    public long size() {
        return this.writePosition + this.cacheFile.valueSize();
    }


    public int read(long position, byte[] bytes) throws IOException {
        int numRead = 0;
        while (numRead < bytes.length) {
            ByteBuffer page;
            if (position >= this.writePosition) {
                page = pageFromCacheFile(position);
                position -= this.writePosition;
            } else {
                page = pageFromFile(position);
            }
            if (page == null) {
                break;
            }
            page.position((int) (position & Constants.PAGE_SIZE_MOD_MASK));
            int size = Math.min(page.remaining(), bytes.length - numRead);
            page.get(bytes, numRead, size);
            numRead += size;
            position += size;
            if (page.limit() < Constants.PAGE_SIZE) {
                break;
            }
        }
        return numRead;
    }

    public ByteBuffer page(long position) throws IOException {
        ByteBuffer page;
        if (position >= this.writePosition) {
            page = pageFromCacheFile(position);
        } else {
            page = pageFromFile(position);
        }
        return page;
    }

    private ByteBuffer pageFromCacheFile(long position) {
        if (position > size()) {
            return null;
        }
        if (cacheView != null) {
            return cacheView.duplicate();
        }
        if (this.cacheFile.getCount() > 0) {
            this.cacheView = this.cacheFile.view();
            return cacheView.duplicate();
        }
        return null;
    }

    private ByteBuffer pageFromFile(long position) throws IOException {
        long localPageNum = (position & Constants.PAGE_SIZE_DIV_MASK);
        if (this.pageNum == -1 || localPageNum != this.pageNum) {
            this.readByteBuffer.clear();
            int numRead;
            if (DirectIOLib.binit) {
                numRead = this.directRandomAccessFile.read(this.readByteBuffer, localPageNum);
            } else {
                numRead = this.fileChannel.read(this.readByteBuffer, localPageNum);
            }
            if (numRead <= 0) {
                return null;
            }
            this.pageNum = localPageNum;
            this.readByteBuffer.flip();
        }
        return this.readByteBuffer;
    }

    public void flush() throws IOException {
        if (DirectIOLib.binit) {

        } else {
            this.fileChannel.force(true);
        }
    }

    public void close() throws IOException {
        this.cacheFile.close();
        if (DirectIOLib.binit) {
            this.directRandomAccessFile.close();
        } else {
            this.fileChannel.close();
        }
    }

    public void recovery(int valueCount) {
        long expectedSize = ((long) valueCount) * Constants.VALUE_SIZE;
        if (this.writePosition >= expectedSize) {
            this.writePosition = expectedSize;
            this.cacheFile.reset(0);
        } else {
            this.writePosition = (this.writePosition & Constants.PAGE_SIZE_DIV_MASK);
            long cachedSize = expectedSize - this.writePosition;
            int cachedNum = (int) (cachedSize / Constants.VALUE_SIZE);
            this.cacheFile.reset(cachedNum);
        }
    }

    public void setWritePosition(long writePosition) {
        this.writePosition = writePosition;
    }

    public int getWriteCacheNum() {
        return this.cacheFile.getCount();
    }
}
