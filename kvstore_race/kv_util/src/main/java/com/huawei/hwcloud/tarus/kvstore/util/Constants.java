package com.huawei.hwcloud.tarus.kvstore.util;

import sun.misc.Unsafe;

public class Constants {

    public static final int KEY_SIZE = 8;

    public static final int VALUE_SIZE = 4 * 1024;

    public static final int PORT = 9527;

    public static final byte SET = 1;

    public static final byte SET_S = 0;

    public static final byte SET_F = 1;

    public static final byte GET = 2;

    public static final byte GET_FULL = 0;

    public static final byte GET_ZERO = 1;

    public static final byte GET_HALF = 2;


    public static final byte META = 3;

    /**
     * 1 << 17 = 128KB
     * 1 << 16 = 64KB
     */
    public static final int PAGE_SIZE_BIT = 17;


    public static final int PAGE_SIZE = 1 << PAGE_SIZE_BIT;

    public static final int PAGE_SIZE_MOD_MASK = PAGE_SIZE - 1;

    public static final int PAGE_SIZE_DIV_MASK = ~(PAGE_SIZE - 1);

    public static final int TOTAL_SIZE = 4000000;

    public static final Unsafe UNSAFE = Tools.unsafe();

    public static final int CACHE_SIZE = 105;

}
