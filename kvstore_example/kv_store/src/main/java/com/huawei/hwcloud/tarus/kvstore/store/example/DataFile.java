package com.huawei.hwcloud.tarus.kvstore.store.example;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

public class DataFile {
	
	private static final Logger log = LoggerFactory.getLogger(DataFile.class);
	
	private String path;
	
	private File file;
	
	private FileChannel fileIo;
	
	public final void init(final String path){
		this.path = path;
		Validate.isTrue(StringUtils.isNotEmpty(this.path), "empty path=[" + this.path + "]");
		
		log.info("init data file=[{}]", this.path);
		
		this.file = new File(this.path);

		try {
			this.fileIo = FileChannel.open(this.file.toPath(),
					StandardOpenOption.CREATE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE);
		} catch (IOException e) {
			throw new KVSException(KVSErrorCode.IO_OPEN_ERROR, KVSErrorCode.IO_OPEN_ERROR.getDescription(), e);
		}
	}
	
	public final void close(){
		flush();
		if(this.fileIo != null){
			try{
				this.fileIo.close();
			}catch(IOException e){
				log.warn("close data file={} error!", path, e);
			}
		}
	}
	
	public final int append(final byte[] buf){
		
		int size = buf.length;
		
		ByteBuffer buffer =  ByteBuffer.allocate(Integer.BYTES + size);
		buffer.putInt(size).put(buf);
		buffer.flip();
		
		//write header 4 byte
		try{
			fileIo.write(buffer);
		}catch(IOException e){
			log.warn("write buf in file=[{}] error!", path, e);
			return 0;
		}

	    return Integer.BYTES + size;
	}

	public final int append(final String info){
		return append(BufferUtil.stringToBytes(info));
	}

	public final int append(final String key, final byte[] val){
		
		byte[] key_bytes = BufferUtil.stringToBytes(key);
		
		int key_size = key_bytes.length;
		int val_size = val.length;
	    
	    ByteBuffer buffer =  ByteBuffer.allocate(Integer.BYTES * 2 + key_size + val_size);
		buffer.putInt(key_size).putInt(val_size).put(key_bytes).put(val);
		buffer.flip();
		
		try{
			fileIo.write(buffer);
		}catch(IOException e){
			log.warn("write kv in file=[{}] error!", path, e);
			return 0;
		}

	    return key_size + val_size + Integer.BYTES * 2;
	}

	public final int read(final int offset, final int size, final Ref<String> out){
		try {
			fileIo.position(offset);
			ByteBuffer buffer =  ByteBuffer.allocate(size);
			int ret = fileIo.read(buffer);
			if(ret <=0){
				return 0;
			}

			out.setValue(BufferUtil.bufferToString(buffer));

			return ret;
		} catch (IOException e) {
			throw new KVSException(KVSErrorCode.IO_READ_ERROR, KVSErrorCode.IO_READ_ERROR.getDescription(), e);
		}
	}

	public final int readKV(final int offset, final Ref<String> key, final Ref<byte[]> val){

		try {
			fileIo.position(offset);
			ByteBuffer buffer =  ByteBuffer.allocate(Integer.BYTES * 2);
			int ret = fileIo.read(buffer);
			if(ret <=0){
				return 0;
			}
			buffer.flip();
			int key_size = buffer.getInt();
			int val_size = buffer.getInt();

			buffer = ByteBuffer.allocate(key_size + val_size);

			ret = fileIo.read(buffer);

			if(ret <=0){
				return 0;
			}
			buffer.flip();

			byte[] key_buf = new byte[key_size];
			byte[] val_buf = new byte[val_size];

			buffer.get(key_buf);
			buffer.get(val_buf);

			key.setValue(BufferUtil.bytesToString(key_buf));
			val.setValue(val_buf);

			return key_size + val_size + Integer.BYTES * 2;
		} catch (IOException e) {
			throw new KVSException(KVSErrorCode.IO_READ_ERROR, KVSErrorCode.IO_READ_ERROR.getDescription(), e);
		}
	}

	public final long size() throws IOException{
	    return fileIo.size();
	}

	public final void flush() {
		if(this.fileIo != null && fileIo.isOpen()){
			try{
				this.fileIo.force(false);;
			}catch(IOException e){
				log.warn("flush data file=[{}] error!", path, e);
			}
		}
    }

    //utils
    public final static boolean remove(final String dir){
    	
    	Validate.isTrue(StringUtils.isNotEmpty(dir), "empty dir=[" + dir + "]");

    	File dirFile = new File(dir);
    	
    	if(!dirFile.exists()){
    		return false;
    	}
    	
        if (dirFile.isFile()) {
        	return dirFile.delete();
        }

        List<String> files = new ArrayList<>();
        
        scanDir(dir, false, files);
        for (String fileName : files) {
        	File file = new File(fileName);
        	file.deleteOnExit();
        }
        return true;
    }

    public final static int scanDir(final String dir, final boolean create_if_no_exist, final List<String> files){
    	
    	File dirFile = new File(dir);
    	
    	if(!dirFile.exists() || !dirFile.isDirectory()){
    		if(create_if_no_exist){
    			dirFile.mkdir();
    		}
    		return 0;
    	}

    	for(File file:dirFile.listFiles()){
    		if(file.exists()){
				try {
					files.add(file.getCanonicalPath());
				} catch (IOException e) {
					throw new KVSException(KVSErrorCode.IO_SCAN_ERROR, KVSErrorCode.IO_SCAN_ERROR.getDescription(), e);
				}
			}
    	}
    	
        return files.size();
    }
    
    public final static String buildPath(final String dir, final String name) {
        return dir + File.separator + name;
    }
}
