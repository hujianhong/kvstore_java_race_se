package com.huawei.hwcloud.tarus.kvstore.service.race;

import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import com.huawei.hwcloud.tarus.kvstore.util.LongIntOpenHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KVService implements KVStoreRace {

    private final LongIntOpenHashMap indexMap = new LongIntOpenHashMap(Constants.TOTAL_SIZE);

    private final LRUCache lruCache = new LRUCache(Constants.CACHE_SIZE);

    private LiteClient liteClient;

    private boolean loadedMeta = false;

    @Override
    public final boolean init(final String dir, final int threadNum) throws KVSException {
        String ip = ResourceManager.getRpcUri().getIp();
        return doInit(ip, threadNum);
    }

    public final boolean doInit(final String ip, final int threadNum) throws KVSException {
        try {
            int port = Constants.PORT + threadNum;
            this.liteClient = new LiteClient(ip, port, lruCache);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int offset = 0;

    private final void loadMeta() throws IOException {
        this.liteClient.meta(this);
        this.loadedMeta = true;
    }

    public final void put(long key, int newOffset) {
        this.indexMap.put(key, newOffset);
        this.offset = newOffset;
    }

    @Override
    public final long set(final String key, final byte[] value) throws KVSException {
        try {
            if (!this.loadedMeta) {
                loadMeta();
            }
            long longKey = keyToLong(key);
            liteClient.set(longKey, value);
            this.offset++;
            this.indexMap.put(longKey, offset);
            return 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer currentPage;

    private long currentPageNum = -1;

    @Override
    public final long get(final String key, final Ref<byte[]> val) throws KVSException {
        if (!this.loadedMeta) {
            try {
                loadMeta();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        long longKey = keyToLong(key);
        int offset = this.indexMap.get(longKey);
        if (offset <= 0) {
            val.setValue(null);
            return -1;
        }
        long position = ((long) (offset - 1)) * Constants.VALUE_SIZE;
        long localPageNum = (position & Constants.PAGE_SIZE_DIV_MASK);
        ByteBuffer page;
        if (currentPageNum == localPageNum) {
            page = currentPage;
        } else {
            page = lruCache.get(localPageNum);
            if (page == null) {
                page = this.liteClient.get(offset);
                if (page != null) {
                    lruCache.put(localPageNum, page);
                    currentPage = page;
                    currentPageNum = localPageNum;
                } else {
                    val.setValue(null);
                    return -1;
                }
            }
        }
        byte[] bytes = new byte[Constants.VALUE_SIZE];
        int pos = (int) (position & Constants.PAGE_SIZE_MOD_MASK);
        Constants.UNSAFE.copyMemory(page.array(), 16 + pos, bytes, 16, Constants.VALUE_SIZE);
        val.setValue(bytes);
        return 1;
    }

    @Override
    public final void close() {
        this.liteClient.close();
    }

    @Override
    public final void flush() {

    }

    public final static long keyToLong(String key) {
        long value = key.charAt(0) - '0';
        for (int i = 1, len = key.length(); i < len; i++) {
            value = (value << 3) + (value << 1) + (key.charAt(i) - '0');
        }
        return value;
    }
}
