package com.huawei.hwcloud.tarus.kvstore.exception;

import org.apache.commons.lang3.ObjectUtils;

import com.huawei.hwcloud.tarus.kvstore.util.UUIDUtil;

public final class KVSException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    private final String trackingId;
    private final int errorCode;    
	private final String errorMessage;
    
    public KVSException(final String message) {
        this(message, null);
    }
    
    public KVSException(final String message, final Throwable cause) {
        this(KVSErrorCode.INTERNAL_SERVER_ERROR, message, cause);
    }
    
    public KVSException(final KVSErrorCode wfsErrorCode) {
        this(wfsErrorCode.getErrorCode(), "", null);
    }
    
    public KVSException(final KVSErrorCode wfsErrorCode, final String message) {
        this(wfsErrorCode.getErrorCode(), message, null);
    }
    
    public KVSException(final KVSErrorCode wfsErrorCode, final String message, final Throwable cause) {
        this(wfsErrorCode.getErrorCode(), String.format("%s; %s;", message, wfsErrorCode.getDescription()), cause);
    }
    
    public KVSException(final int errorCode, final String message) {
        this(errorCode, message, null);
    }
    
    public KVSException(final int errorCode, final String message, final Throwable cause) {
        this(UUIDUtil.generate(), errorCode, message, cause);
    }
    
    private KVSException(final String trackingId, final int errorCode, final String message, final Throwable cause) {
        super(String.format("%s; trackingId=%s; errorCode=%s;", message, trackingId, errorCode), cause);
        this.trackingId = ObjectUtils.firstNonNull(trackingId, UUIDUtil.generate());
        this.errorCode = errorCode;
        this.errorMessage = super.getMessage();
    }

	public final String getTrackingId() {
		return trackingId;
	}
	
	public final int getErrorCode() {
		return errorCode;
	}

	public final String getErrorMessage() {
		return errorMessage;
	}
}
