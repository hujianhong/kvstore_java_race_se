package com.huawei.hwcloud.tarus.kvstore.service.example;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class KVService implements KVStoreRace {
	
	private static final Logger log = LoggerFactory.getLogger(KVService.class);

    private MetaDataManager metaDataManager = new MetaDataManager();
    
    private DataAgent data = new DataAgent();

	@Override
	public boolean init(final String dir, final int thread_num) throws KVSException {
		try {
			data.init();
			metaDataManager.init(data);
		} catch (IOException e) {
			throw new KVSException(KVSErrorCode.INIT_RACE_ERROR, KVSErrorCode.INIT_RACE_ERROR.getDescription(), e);
		}
	    return true;
	}

	@Override
	public long set(final String key, final byte[] val) throws KVSException {
		long pos = -1;
		try {
			pos = data.append(ConfigManager.KV_OP_DATA_APPEND, key, val);
			metaDataManager.set(key, pos);
			return (pos >> 32);
		} catch (IOException e) {
			throw new KVSException(KVSErrorCode.SET_RACE_ERROR, KVSErrorCode.SET_RACE_ERROR.getDescription(), e);
		}
	}

	@Override
	public long get(final String key, final Ref<byte[]> val) throws KVSException {
		
		try {
			long pos = metaDataManager.get(key);
			final Ref<String> key_ref = Ref.of(String.class);
			key_ref.setValue(key);
			data.get(ConfigManager.KV_OP_DATA_GET, pos, key_ref, val);
			return (pos >> 32);
		} catch (IOException e) {
			throw new KVSException(KVSErrorCode.GET_RACE_ERROR, KVSErrorCode.GET_RACE_ERROR.getDescription(), e);
		}
	}

	@Override
	public void close() {
		data.close();
		metaDataManager.close();
	}

	@Override
	public void flush() {

	}
}
