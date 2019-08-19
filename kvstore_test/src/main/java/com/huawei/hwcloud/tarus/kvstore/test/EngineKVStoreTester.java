package com.huawei.hwcloud.tarus.kvstore.test;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreCheck;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;

public class EngineKVStoreTester implements KVStoreCheck {

	private static final Logger log = LoggerFactory.getLogger(EngineKVStoreTester.class);
	
	private static final int DEFAULT_KV_KEY_Times = 1000000;
	
	private int kvKeyTimes = DEFAULT_KV_KEY_Times;
	
	@Override
	public double execute() {
		
		int kv_num = ResourceManager.getKvNumPerThread();

	    if (kv_num < 0 || kv_num > 64) {
	    	log.error("parameter error, 0 < thread_num < 64 && 0 < kv_num < 64");
	        return -1;
	    }
	    
	    log.info("begin check with key size=8, val size=4096, KV number=[{}M/thread]!", kv_num);

	    String dir = ResourceManager.buildFullDir(ConfigManager.DATA_FILE_DIR);
	    
	    remove_files(dir);
	    
	    SimpleCase tester = new SimpleCase();
	    tester.init();

	    Ref<Integer> error = Ref.of(Integer.class);
	    error.setValue(0);
	    double time = tester.test(dir, kv_num * kvKeyTimes, error);
	    
	    log.info("Time=[{}ms], Error=[{}]", time, error.getValue());

	    tester.uninit();
	    return time;
	}
	
	public void setKvKeyTimes(final int kvKeyTimes){
		this.kvKeyTimes = kvKeyTimes;
	}
	
	private void remove_files(final String dir) {
		
		File dirFile = new File(dir);
		
		if(!dirFile.exists()){
			dirFile.mkdirs();
			return;
		}else if(!dirFile.isDirectory()){
			dirFile.delete();
			dirFile.mkdirs();
			return;
		}
		
		for(File file:dirFile.listFiles()){
			file.delete();
    	}
	}
}
