package com.huawei.hwcloud.tarus.kvstore.service.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.hwcloud.tarus.kvstore.common.Ref;

public class MetaDataManager {
	
	private static final Logger log = LoggerFactory.getLogger(MetaDataManager.class);

	private DataAgent agent;

	private String path;
	
	private final Map<String, Long> keys_map = new HashMap<>();
	
	public final boolean init(final DataAgent agent) throws IOException{
		close();

		this.agent = agent;
	    restoreMeta();
	    return true;
	}

	public final void set(final String key, final long pos) throws IOException{
		keys_map.put(key, pos);
		agent.append(ConfigManager.KV_OP_META_APPEND, key, String.valueOf(pos).getBytes());
	}

	public final long get(final String key){
		if(keys_map.containsKey(key)){
			return keys_map.get(key);
		}else{
			return 0;
		}
	}

	public final int restoreMeta() throws NumberFormatException, IOException{
		
		final Ref<String> key = Ref.of(String.class);
		final Ref<byte[]> val = Ref.of(byte[].class);
	    int pos = 0;
	    final int int_size = Long.BYTES;
	    int readSize=0;

	    while (true){
			readSize=agent.get(ConfigManager.KV_OP_META_GET, pos, key, val);
			log.info("restore meta data, readSize=[{}]", readSize);
			if(readSize <= int_size){
				log.info("read meta end, readSize=[{}]!", readSize);
				break;
			}
			keys_map.put(key.getValue(), Long.parseLong(new String(val.getValue())));
			pos += readSize;
		}
	    return keys_map.size();
	}
	
	public final void close(){
		keys_map.clear();
	}
}
