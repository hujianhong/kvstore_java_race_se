package com.huawei.hwcloud.tarus.kvstore.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.text.DecimalFormat;

import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;

public class BufferUtil {
	
	private static final String DEFAULT_ENCODING = "UTF-8";
	
	private static final String FILE_NAME_FORMAT = "0000000000";
	
	private static final String THREAD_PATH_FORMAT = "00";
	
	public static final byte[] stringToBytes(final String str){
		try {
			return str.getBytes(DEFAULT_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new KVSException(KVSErrorCode.INTERNAL_SERVER_ERROR, 
					KVSErrorCode.INTERNAL_SERVER_ERROR.getDescription(), 
					e);
		}
	}
	
	public static final ByteBuffer stringToBuffer(final String str){
		Charset charset = Charset.forName(DEFAULT_ENCODING) ;
		CharsetEncoder encoder = charset.newEncoder() ;
		CharBuffer cb = CharBuffer.wrap(str) ;
		ByteBuffer buf;
		try {
			buf = encoder.encode(cb);
			buf.flip();
			return buf;
		} catch (CharacterCodingException e) {
			throw new KVSException(KVSErrorCode.INTERNAL_SERVER_ERROR, 
					KVSErrorCode.INTERNAL_SERVER_ERROR.getDescription(), 
					e);
		}		
	}
	
	public static final String bytesToString(final byte[] bytes){
		return bufferToString(ByteBuffer.wrap(bytes));
	}
	
	public static final String bufferToString(final ByteBuffer buf) {
		Charset charset = Charset.forName(DEFAULT_ENCODING) ;
		CharsetDecoder decoder = charset.newDecoder();
		CharBuffer cb;
		try {
			cb = decoder.decode(buf);
			return cb.toString();
		} catch (CharacterCodingException e) {
			throw new KVSException(KVSErrorCode.INTERNAL_SERVER_ERROR, 
					KVSErrorCode.INTERNAL_SERVER_ERROR.getDescription(), 
					e);
		}		
	}	
	
	public static final String fillNo(final long no){
		DecimalFormat df = new DecimalFormat(FILE_NAME_FORMAT);
		return df.format(Long.valueOf(no));
	}
	
	public static final String fillThreadNo(final int no){
		DecimalFormat df = new DecimalFormat(THREAD_PATH_FORMAT);
		return df.format(Integer.valueOf(no));
	}
}
