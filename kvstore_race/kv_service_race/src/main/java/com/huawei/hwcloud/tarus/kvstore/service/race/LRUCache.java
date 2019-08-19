package com.huawei.hwcloud.tarus.kvstore.service.race;

import com.huawei.hwcloud.tarus.kvstore.util.Constants;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class LRUCache extends LinkedHashMap<Long, ByteBuffer> {
    private final int capacity;

    private final Queue<ByteBuffer> queue;

    public LRUCache(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
        this.queue = new LinkedList<>();
    }

    @Override
    public final boolean removeEldestEntry(Map.Entry<Long, ByteBuffer> eldest) {
        boolean result = size() > capacity;
        if (result) {
            this.queue.add(eldest.getValue());
        }
        return result;
    }

    public final ByteBuffer getByteBuffer() {
        if (queue.isEmpty()) {
            return ByteBuffer.allocate(Constants.PAGE_SIZE);
        }
        return queue.poll();
    }
}
