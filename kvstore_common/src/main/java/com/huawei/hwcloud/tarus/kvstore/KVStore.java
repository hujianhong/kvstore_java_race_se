package com.huawei.hwcloud.tarus.kvstore;

import com.huawei.hwcloud.tarus.kvstore.common.RaceManager;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;

public class KVStore {

	public static final void main(String[] args) {
		System.out.println("begin to start race...");
		ResourceManager.init(args);
		ExecuteCheck();
		System.out.println("started race finish...");		
	}

	private final static void ExecuteCheck() {
		System.out.println("begin to execute check...");
		RaceManager.instance().getChecker().execute();
		System.out.println("executed check finish...");
	}
}
