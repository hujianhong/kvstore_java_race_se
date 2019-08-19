package com.huawei.hwcloud.tarus.kvstore.store.race;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.KVStoreCheck;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.util.Constants;

public class KVStoreServer implements KVStoreCheck {

	private static final int THREAD_NUMS = 16;

	@Override
	public double execute() {
		String dataDir = ResourceManager.buildFullDir(ConfigManager.DATA_FILE_DIR);
		for (int i = 0; i < THREAD_NUMS; i++) {
			final int threadNum = i;
			new Thread(() -> {
				try {
					Bucket bucket = new Bucket(dataDir, threadNum);
					bucket.init();
					LiteServer liteServer = new LiteServer(bucket, Constants.PORT + threadNum);
					liteServer.service();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}).start();
		}
		return 0.1;
	}
}
