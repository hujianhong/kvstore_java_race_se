package com.huawei.hwcloud.tarus.kvstore.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;

public class RaceManager {
	
	private static final Logger log = LoggerFactory.getLogger(RaceManager.class);
	
	private static final RaceManager manager =  new RaceManager();
	
	private KVStoreRace racer= null;
	
	private KVStoreCheck checker= null;
	
	public static RaceManager instance(){
		return manager;
	}
	
	public final void registerRacer(final KVStoreRace racer){
		this.racer= racer;
	}
	
	public final void registerChecker(final KVStoreCheck checker){
		this.checker= checker;
	}
	
	public final KVStoreRace getRacer(){
		if(this.racer == null){
			String message = "racer is null!";
			log.error(message);
			throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR, message);
		}
		
		return this.racer;
	}
	
	public final KVStoreCheck getChecker(){
		if(this.checker == null){
			String message = "checker is null!";
			log.error(message);
			throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR, message);
		}
		
		return this.checker;
	}
}
