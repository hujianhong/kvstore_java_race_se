package com.huawei.hwcloud.tarus.kvstore.store.race;

public class ServerMain {

    public static void main(String[] args) {
        KVStoreServer kvStoreServer = new KVStoreServer();
        kvStoreServer.execute();
    }
}
