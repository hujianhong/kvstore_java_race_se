package com.huawei.hwcloud.tarus.kvstore.service.example;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataAgent {
	
	private static final Logger log = LoggerFactory.getLogger(DataAgent.class);

	private TcpClient client = null;

	public final int init() throws IOException {
		release();
		client = TcpClient.createConnect(ResourceManager.getRpcUri());
    	return 0;
    }

    public final void release( ) {
		if(client != null){
			client.close();
		}
    }

    public final void close() {
    	release();
    }

    public final long append(final byte type, final String key, final byte[] val) throws IOException {

		log.info("append data, key=[{}], val size=[{}]", key, val.length);

		byte[] key_bytes = BufferUtil.stringToBytes(key);

		int key_size = key_bytes.length;
		int val_size = val.length;

		ByteBuffer buffer =  ByteBuffer.allocate(Byte.BYTES + Integer.BYTES * 2 + key_size + val_size);
		buffer.put(type).putInt(key_size).putInt(val_size).put(key_bytes).put(val);
		buffer.flip();

		byte[] ret = client.send(buffer.array());
		if(ret == null){
			return -1;
		}
		buffer =  ByteBuffer.wrap(ret);
		long pos = buffer.getLong();

    	return pos;
    }

    public final int get(final byte type, final long pos, final Ref<String> key, final Ref<byte[]> val) throws IOException {

		log.info("req pos is=[{}]", pos);

		ByteBuffer buffer =  ByteBuffer.allocate(Byte.BYTES + Long.BYTES);
		buffer.put(type).putLong(pos);
		byte[] req = buffer.array();

		log.info("req is=[{}]", BufferUtil.bytesToString(req));

		byte[] res = client.send(req);
		if(res == null){
			log.error("response message is null!");
			return -1;
		}
		buffer = ByteBuffer.allocate(res.length);
		buffer.put(res);
		buffer.flip();
		int key_size = buffer.getInt();
		if(key_size < 0){
			return key_size;
		}
		int val_size = buffer.getInt();
		byte[] key_buf = new byte[key_size];
		buffer.get(key_buf);
		byte[] val_buf = new byte[val_size];
		buffer.get(val_buf);
		key.setValue(BufferUtil.bytesToString(key_buf));
		val.setValue(val_buf);
		return key_size + val_size;
    }
}
