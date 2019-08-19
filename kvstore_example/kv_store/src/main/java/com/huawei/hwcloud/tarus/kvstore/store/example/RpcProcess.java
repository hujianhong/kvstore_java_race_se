package com.huawei.hwcloud.tarus.kvstore.store.example;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.common.ResourceManager;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RpcProcess {

    private static final Logger log = LoggerFactory.getLogger(RpcProcess.class);

    private static final int errMsg = -1;

    private DataManager meta = new DataManager();

    private DataManager data = new DataManager();

    private String dir;

    public boolean init(final String dir) {
        this.dir = dir;
        Validate.isTrue(StringUtils.isNotEmpty(this.dir), "empty dir=[" + this.dir + "]");
        if(ResourceManager.getClearDataFlag()){
            data.clear();
            meta.clear();
        }

        try {
            data.init(this.dir, ConfigManager.DATA_FILE_SUFFIX, DataManager.MAX_FILE_SIZE);
            meta.init(this.dir, ConfigManager.META_FILE_SUFFIX, DataManager.MAX_FILE_SIZE);
        } catch (IOException e) {
            throw new KVSException(KVSErrorCode.INIT_RACE_ERROR, KVSErrorCode.INIT_RACE_ERROR.getDescription(), e);
        }

        return true;
    }

    public void stop() {
        try {
            Thread.currentThread().sleep(1 * 1000);
        } catch (InterruptedException e) {
            throw new KVSException(KVSErrorCode.RPC_CLOSE_ERROR,
                    "close all tcp client error!");
        }
    }

    public byte[] process(byte[] req) {

        ByteBuffer buffer =  ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(errMsg);
        byte[] res = buffer.array();

        if(req.length < (Integer.BYTES + 1)){
            log.error("request message length=[{}], it is too less!", req.length);
            return res;
        }

        byte type = req[0];

        switch(type) {
            case ConfigManager.KV_OP_META_APPEND:
                log.info("process message for append meta!");
                res = processAppend(meta, req);
                break;

            case ConfigManager.KV_OP_META_GET:
                log.info("process message for get meta!");
                res = processGet(meta, req);
                break;

            case ConfigManager.KV_OP_DATA_APPEND:
                log.info("process message for append data!");
                res = processAppend(data, req);
                break;

            case ConfigManager.KV_OP_DATA_GET:
                log.info("process message for get data!");
                res = processGet(data, req);
                break;

            case ConfigManager.KV_OP_CLEAR:
                //TODO: clear local data
                break;

            default:
                log.error("unknown rpc type=[{}]", type);
                break;
        }
        return res;
    }

    public byte[] processAppend(final DataManager dataManager, final byte[] buf) {

        ByteBuffer buffer =  ByteBuffer.wrap(buf, 1, buf.length - 1);

        int key_size = buffer.getInt();
        int val_size = buffer.getInt();

        byte[] key_buf = new byte[key_size];
        buffer.get(key_buf);
        byte[] val_buf = new byte[val_size];
        buffer.get(val_buf);

        String key = BufferUtil.bytesToString(key_buf);

        log.info("process append, key is=[{}], val_size is=[{}]", key, val_size);

        long pos = dataManager.append(key, val_buf);

        buffer =  ByteBuffer.allocate(Long.BYTES);
        buffer.clear();
        buffer.putLong(pos);

        return buffer.array();
    }

    public byte[] processGet(final DataManager dataManager, byte[] buf) {

        ByteBuffer buffer =  ByteBuffer.wrap(buf, 1, buf.length - 1);
        long pos = buffer.getLong();

        log.info("process get, pos is=[{}]", pos);

        final Ref<String> key_ref = Ref.of(String.class);
        final Ref<byte[]> val_ref = Ref.of(byte[].class);
        int ret = dataManager.get(pos, key_ref, val_ref);
        if(ret < 0){
            log.error("get data error, ret is=[{}]", ret);
            buffer =  ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(errMsg);
            return buffer.array();
        }

        String key = key_ref.getValue();
        if(StringUtils.isEmpty(key)){
            log.error("get data error, key is null!");
            buffer =  ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(errMsg);
            return buffer.array();
        }
        byte[] val_buf = val_ref.getValue();
        if(val_buf == null){
            log.error("get data error, val is null!");
            buffer =  ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(errMsg);
            return buffer.array();
        }

        byte[] key_bytes = BufferUtil.stringToBytes(key);

        int key_size = key_bytes.length;
        int val_size = val_buf.length;

        log.info("process append, key is=[{}], val_size is=[{}]", key, val_size);

        buffer =  ByteBuffer.allocate(Integer.BYTES * 2 + key_size + val_size);
        buffer.putInt(key_size).putInt(val_size).put(key_bytes).put(val_buf);
        buffer.flip();

        return buffer.array();
    }

    public void close() {
        data.close();
        meta.close();
    }
}
