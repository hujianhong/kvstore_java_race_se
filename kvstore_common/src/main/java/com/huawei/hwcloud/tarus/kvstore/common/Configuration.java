package com.huawei.hwcloud.tarus.kvstore.common;

public enum Configuration {
    
    KVS_CONFIG_USER_DIR("user.dir", StringValue.Empty),
    KVS_CONFIG_HOME("kvs.home", StringValue.Empty),
    
    KVS_CONFIG__KVS_EXECUTE_MODE("kvs.execute.mode", "race"),
    KVS_CONFIG__KVS_CHECK_MODE("kvs.check.mode", StringValue.Empty),
    
    KVS_CONFIG__KVS_PER_THREAD_KV_NUM("kvs.per.thread.kv.num", "1"),
    KVS_CONFIG__KVS_THREAD_NUM("kvs.thread.num", StringValue.Zero),

    KVS_CONFIG_KVS_RPC_URL("kvs.rpc.uri", "tcp://127.0.0.1:9527"),
    KVS_CONFIG_KVS_CLEAR_DATA("kvs.clear.data", StringValue.Zero),
        
    ;
    
    private final String paramName;
    private final String defaultValue;
        
    private Configuration(final String paramName, final String defaultValue){
        this.paramName = paramName;
        this.defaultValue = defaultValue;
    }
    
    public final String getParamName(){
        return this.paramName;
    }
    
    public final String getDefaultValue(){
        return this.defaultValue;
    }
}
