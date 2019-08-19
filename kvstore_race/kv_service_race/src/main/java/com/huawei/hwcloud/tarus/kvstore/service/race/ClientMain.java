package com.huawei.hwcloud.tarus.kvstore.service.race;

import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.util.Constants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.util.Arrays;

public class ClientMain {

    public static void main(String[] args) {
        int threadNums = 16;
        for (int k = 0; k < threadNums; k++) {
            final int threadNum = k;
            new Thread(() -> {
                KVService kvService = new KVService();
                kvService.doInit("localhost", threadNum);
                int maxItem = 1000;
                long[] keys = new long[maxItem];
                byte[][] values = new byte[maxItem][];

                for (int i = 0; i < maxItem; i++) {
                    keys[i] = RandomUtils.nextLong(Integer.MAX_VALUE, Integer.MAX_VALUE * 8L);
                    values[i] = RandomStringUtils.randomAlphabetic(Constants.VALUE_SIZE).getBytes();
                }
                long s1 = System.currentTimeMillis();
                for (int i = 0; i < maxItem; i++) {
                    kvService.set(keys[i] + "", values[i]);
                }
                long s2 = System.currentTimeMillis() - s1;
                System.out.println(threadNum + " flush,cost:" + s2);
                Ref<byte[]> value = Ref.of(null);
                long s3 = System.currentTimeMillis();
                for (int i = 0; i < maxItem; i++) {
                    long code = kvService.get(keys[i] + "", value);
                    if (code > 0) {
                        if (!Arrays.equals(values[i], value.getValue())) {
                            throw new IllegalStateException("not find:" + keys[i]);
                        }
                    } else {
                        throw new IllegalStateException("not find:" + keys[i]);
                    }
                }
                long s4 = System.currentTimeMillis() - s3;
                System.out.println(threadNum + " get,cost:" + s4);
                kvService.close();
            }).start();
        }

    }
}
