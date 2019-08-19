package com.huawei.hwcloud.tarus.kvstore.common;

public enum ExecuteMode {
	
	KVSERVICE_EXAMPLE("kvservice.example", "com.huawei.hwcloud.tarus.kvstore.service.example.KVService"),
    KVSERVICE_RACE("kvservice.race", "com.huawei.hwcloud.tarus.kvstore.service.race.KVService"),
    KVSTORE_EXAMPLE("kvstore.example", "com.huawei.hwcloud.tarus.kvstore.store.example.KVStoreServer"),
    KVSTORE_RACE("kvstore.race", "com.huawei.hwcloud.tarus.kvstore.store.race.KVStoreServer"),
	
	;
    
    private final String modeName;
    private final String modeClazz;

    private ExecuteMode(final String modeName, final String modeClazz){
        this.modeName = modeName;
        this.modeClazz = modeClazz;
    }
    
    public final String getModeName(){
        return this.modeName;
    }
    
    public final String getModeClazz(){
        return this.modeClazz;
    }
}
