package com.huawei.hwcloud.tarus.kvstore.exception;

public enum KVSErrorCode {
    
    // comm error 100001~100100
    INTERNAL_SERVER_ERROR(100001, "Internal error"),
    UNSUPPORTED_MODE_ERROR(100002, "unsupported mode error"),
    
    // config error 100101~100200
    CONFIG_PARAM_ERROR(100101, "param error"),
    
    // race error 100201~100300
    INIT_RACE_ERROR(100201, "init race error"),
    SET_RACE_ERROR(100202, "set race error"),
    GET_RACE_ERROR(100203, "get race error"),
    RPC_CONNECT_ERROR(100204, "rpc connect error"),
    RPC_LISTEN_ERROR(100205, "rpc write error"),
    RPC_READ_ERROR(100206, "rpc read error"),
    RPC_CLOSE_ERROR(100207, "rpc close error"),
    IO_SCAN_ERROR(100208, "io scan error"),
    IO_OPEN_ERROR(100209, "io open error"),
    IO_READ_ERROR(100210, "io read error"),
    IO_CLOSE_ERROR(100211, "io close error"),
    IO_LISTEN_ERROR(100212, "io listen error"),
    
    // test error 100301~100400
   TEST_EXECUTE_ERROR(100301, "test execute error"),
   TEST_REMOVE_ERROR(100302, "test remove error"),
    
    ;

    private final int errorCode;
    private final String description;
    
    KVSErrorCode(final int errorCode) {
        this(errorCode, "");
    }
    
    KVSErrorCode(final int errorCode, final String description) {
        this.errorCode = errorCode;
        this.description = description;
    }
    
    public final int getErrorCode(){
    	return errorCode;
    }
    
    public final String getDescription(){
    	return description;
    }
}
