package com.huawei.hwcloud.tarus.kvstore.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.hwcloud.tarus.kvstore.common.KVStoreRace;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

public class SimpleCase {

	private static final Logger log = LoggerFactory.getLogger(EngineKVStoreTester.class);
	
	private final List<KVStoreRace> kv_racers = new ArrayList<>();
	
	private ExecutorService executor;
	private int thread_num;
	
	public boolean init() {
		executor = Executors.newFixedThreadPool(ResourceManager.getThreadNum());
		thread_num = ResourceManager.getThreadNum();

	    if (thread_num < 0 || thread_num > 64) {
	    	log.error("parameter error, 0 < thread_num < 64");
	    	return false;
	    }
	    
	    kv_racers.clear();
		
		int init_num;
	    
	    for (init_num= 0; init_num < thread_num; init_num ++) {
	    	KVStoreRace racer = ResourceManager.initExecute();
	    	kv_racers.add(racer);
	    }
	    
		return true;
	}

	public double test(final String dir, final int times,  final Ref<Integer> err) {
		
    	List<Integer> prefixs = new ArrayList<>();
    	List<FutureTask<Integer>> rets = new ArrayList<>();
    	
    	try{
    		
    		int base = Calendar.getInstance().hashCode();
    	    for (int i = 0; i < thread_num; i ++) {
    	    	prefixs.add(i + base);
    	    }
    		    		
    	    long begin = System.currentTimeMillis();
    	    
    	    for (int i = 0; i < thread_num; i ++) {
    	    	
    	    	final int num = i;
    	    	
    	    	FutureTask<Integer> future = new FutureTask<Integer>(new Callable<Integer>() {
    	    		public Integer call() {
    	    			return jobWrite(kv_racers.get(num), num, prefixs.get(num), dir, times);
    	    		}
    	    	});
    	    	executor.submit(future);
    	    	rets.add(future);
    	    }

    	    for (int i = 0; i < thread_num; i ++){
    	    	try {
    	    		int result = rets.get(i).get();
    				err.setValue(result+ err.getValue());
    			} catch (InterruptedException | ExecutionException e) {
    				throw new KVSException(KVSErrorCode.TEST_EXECUTE_ERROR, 
    						KVSErrorCode.TEST_EXECUTE_ERROR.getDescription(), 
    						e);
    			}
    	    }
    	    
    	    for (int i = 0; i < thread_num; i ++) {
    	    	
    	    	final int num = i;
    	    	
    	    	FutureTask<Integer> future = new FutureTask<Integer>(new Callable<Integer>() {
    	    		public Integer call() {
    	    			return jobRead(kv_racers.get(num), num, prefixs.get(num), dir, times); 
    	    		}
    	    	});
    	    	executor.submit(future);
    	    	rets.add(future);
    	    }

    	    for (int i = 0; i < thread_num; i ++){
    	    	try {
    	    		int result = rets.get(i).get() ;
    				err.setValue(result + err.getValue());
    			} catch (InterruptedException | ExecutionException e) {
    				throw new KVSException(KVSErrorCode.TEST_EXECUTE_ERROR, 
    						KVSErrorCode.TEST_EXECUTE_ERROR.getDescription(), 
    						e);
    			}
    	    }
    	    
    	    long end = System.currentTimeMillis();
    	    
    	    return (end - begin);
    	}catch(Throwable t){
    		t.getStackTrace();
    		log.error("perf test error", t);
    		return -1;
    	}
	}
	
	public void uninit() {
		for (KVStoreRace racer : kv_racers) {
			racer.close();
	    }
		kv_racers.clear();
		if(executor != null){
			executor.shutdownNow();
			executor = null;
		}
	}
	
	private final int jobWrite(final KVStoreRace store, final int thread_num, final int prefix, final String dir, final int times) {
		Ref<Integer> error = Ref.of(Integer.class);
		error.setValue(0);

		store.init(dir, thread_num);
		write(store, prefix, times, error);
	    store.close();

	    return error.getValue();
	}

	private final int jobRead(final KVStoreRace store, final int thread_num, final int prefix, final String dir, final int times) {
		Ref<Integer> error = Ref.of(Integer.class);
		error.setValue(0);
	    
	    store.init(dir, thread_num);
	    read(store, prefix, times, error);
	    store.close();

	    return error.getValue();
	}

	public void write(final KVStoreRace store, final int prefix, final int times, final Ref<Integer> err) {
		
		long base = prefix;
	    base <<= 32;
		
		for (int i = 0; i < times; i ++) {
	        String key = buildKey(i+base);
	        String val = buildVal(i+1);
	        store.set(key, BufferUtil.stringToBytes(val));
	    }

	    for (int i = 0; i < times; i ++) {
	        String key = buildKey(i+base);
	        String val = buildVal(i+1);
	        Ref<byte[]> val_ref = Ref.of(byte[].class);
	        store.get(key, val_ref);
	        
	        if (val_ref.getValue() == null) {
	        	err.setValue(err.getValue() + 1);
	            log.error("get key=[{}] error, real val is null!", key);
	            break;
	        }
	        
	        if (!(Arrays.equals(val.getBytes(), val_ref.getValue())) ) {
	            err.setValue(err.getValue() + 1);
	            log.error("get key=[{}] error, expect val=[{}], real val=[{}]", key, val.length(), BufferUtil.bytesToString(val_ref.getValue()));
	            break;
	        }
	    }
	}
	
	public void read(final KVStoreRace store, final int prefix, final int times, final Ref<Integer> err) {
		
		long base = prefix;
	    base <<= 32;
		
		for (int i = 0; i < times; i ++) {
	        String key = buildKey(i+base);
	        String val = buildVal(i+1);
	        Ref<byte[]> val_ref = Ref.of(byte[].class);
	        store.get(key, val_ref);
	       
	        if (val_ref.getValue() == null) {
	        	err.setValue(err.getValue() + 1);
	            log.error("get key=[{}] error, real val is null!", key);
	            break;
	        }
	        
	        if (!(Arrays.equals(val.getBytes(), val_ref.getValue())) ) {
	            err.setValue(err.getValue() + 1);
	            log.error("get key=[{}] error, expect val=[{}], real val=[{}]", key, val.length(), BufferUtil.bytesToString(val_ref.getValue()));
	            break;
	        }
	    }
	}
	
	private final String buildKey(final long i) {
		return String.format("%d", i);
	}
	
	private final String buildVal(final int i) {
		return String.format("hello_%d", i);
	}
}
