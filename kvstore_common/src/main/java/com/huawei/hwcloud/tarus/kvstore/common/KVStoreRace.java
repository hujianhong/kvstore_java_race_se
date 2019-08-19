package com.huawei.hwcloud.tarus.kvstore.common;

import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;

public interface KVStoreRace {
	
	/**
	 * init Engine
	 * @param path the path of engine store data. 
	 * @throws EngineException 
	 */
	public boolean init(final String dir, final int thread_num) throws KVSException;
	
	/**
	 *  write a key-value pair into engine
	 * @param key
	 * @param value
	 * @throws EngineException
	 */
	public long set(final String key, final byte[] value) throws KVSException;
	
	/**
	 * read value of a key
	 * @param key
	 * @return value
	 * @throws EngineException
	 */
	public long get(final String key, final Ref<byte[]> val) throws KVSException;
	
	/**
	 * close Engine
	 */
	public void close();
	
	public void flush();
}
