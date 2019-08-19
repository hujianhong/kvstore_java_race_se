package com.huawei.hwcloud.tarus.kvstore.store.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class Bucket {
    private final int bucketId;
    private final String dir;

    private IndexFile indexFile;

    private DataFile dataFile;

    public Bucket(final String dir, final int bucketId) {
        this.dir = dir;
        this.bucketId = bucketId;
    }


    public boolean init() throws IOException {
        File dirFile = new File(this.dir);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }

        String indexFilePath = this.dir + "/" + this.bucketId + ".key";
        String dataFilePath = this.dir + "/" + this.bucketId + ".value";
        String cacheFilePath = this.dir + "/" + this.bucketId + ".cache";

        File temFile = new File(indexFilePath);
        String kvsClearData = System.getProperty("kvs.clear.data");
//        LOG.info("kvs.clear.data:{}", kvsClearData);
        if (("0".equals(kvsClearData) && temFile.exists())) {
            temFile.delete();
            new File(dataFilePath).delete();
            new File(cacheFilePath).delete();
        }

        this.indexFile = new IndexFile(indexFilePath);
        int valueCount = this.indexFile.getCount();

        this.dataFile = new DataFile(dataFilePath, cacheFilePath);
        this.dataFile.recovery(valueCount);
        return true;
    }

    public void set(final long key, final ByteBuffer buffer) throws IOException {
        this.dataFile.write(buffer);
        this.indexFile.writeLong(key);
    }

    public IndexFile getIndexFile() {
        return this.indexFile;
    }

    public ByteBuffer page(int offset) throws IOException {
        long position = ((long) (offset - 1)) * Constants.VALUE_SIZE;
        return this.dataFile.page(position);
    }

    public void close() {
        try {
            this.dataFile.close();
        } catch (IOException e) {
           e.printStackTrace();
        }
    }

    public void flush() {
        try {
            this.indexFile.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            this.dataFile.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
