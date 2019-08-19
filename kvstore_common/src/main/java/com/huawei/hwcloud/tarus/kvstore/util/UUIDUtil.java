package com.huawei.hwcloud.tarus.kvstore.util;

import java.util.UUID;

/**
 * Utilities to manipulate UUID
 */
public class UUIDUtil {
    public static final String UNKNOWN_TAG = "00";
    
    public static String generate() {
        return UUID.randomUUID().toString();
    }
    
    public static String generateUUIDLength32() {
    	UUID uuid = UUID.randomUUID();
        return uuid.toString().replaceAll("\\-", "");
    }    
}
