package com.huawei.hwcloud.tarus.kvstore.store.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.hwcloud.tarus.kvstore.common.ConfigManager;
import com.huawei.hwcloud.tarus.kvstore.common.Ref;
import com.huawei.hwcloud.tarus.kvstore.util.BufferUtil;

public class DataManager {
	
	private static final Logger log = LoggerFactory.getLogger(DataManager.class);

	public final static int MAX_FILE_SIZE = (1 << (10 * 2 + 8)); //default size: 256MB per file
	
	private int kMaxFileSize = MAX_FILE_SIZE;
	
	private String dir;
	private String ext;
	
	private Map<Integer, DataFile> datafiles = new HashMap<>();
	
	private int cur_file_no   = 0;
    private long cur_file_size = 0;
	
    public final int init(final String dir, final String ext, final int file_size) throws IOException {
    	log.info("init data file=[{}], file size=[{}]", dir, file_size);
    	release();
    	kMaxFileSize = file_size;
    	this.dir = dir;
    	this.ext = ext;
    	Validate.isTrue(StringUtils.isNotEmpty(this.dir), "empty dir=[" + this.dir + "]");
		Validate.isTrue(StringUtils.isNotEmpty(this.ext), "empty ext=[" + this.ext + "]");
    	
    	if (scanDir(this.dir) < 1) {
    		newFile();
    	}
    	return 0;
    }

    public final void release( ) {
    	datafiles.clear();
    }

	public void clear() {
		release();
		if (StringUtils.isNotEmpty(this.dir)) {
			DataFile.remove(this.dir);
		}

		cur_file_no    = -1;
		cur_file_size  = 0;
	}

    public final void close() {
    	if (StringUtils.isNotEmpty(this.dir) && !datafiles.isEmpty()) {
    		datafiles.values().forEach(s -> s.close());
    	}
    	release();
    }

    public final long append(final String key, final byte[] val) {
    	long pos = cur_file_no;
    	pos <<= 32;
    	pos += cur_file_size;

    	int size = datafiles.get(cur_file_no).append(key, val);
    	if (size > 0) {
    		cur_file_size += size;
    	}
    	datafiles.get(cur_file_no).flush();
    	if (cur_file_size > kMaxFileSize) {
    		newFile();
    	}

    	return pos;
    }

    public final void newFile() {
    	cur_file_no++;
    	
    	DataFile dataFile = new DataFile();
    	
		dataFile.init(no2FileName(cur_file_no, ext));
    	
    	datafiles.put(Integer.valueOf(cur_file_no), dataFile);    	
    	cur_file_size = 0;
    }

    public final int get(final long pos, final Ref<String> key, final Ref<byte[]> val) {
    	int no     = (int) ((pos >> 32) & 0xffffffff);
    	int offset = (int) (pos & 0xffffffff);
    	if (datafiles.containsKey(no)) {
    		return datafiles.get(no).readKV(offset, key, val);
    	} else {
    		return -1;
    	}
    }

    public final int scanDir(final String dir) throws IOException {
    	
    	List<String> files = new ArrayList<>();
    	
    	int num = DataFile.scanDir(dir, true, files);
    	if (num < 1) {
    		return 0;
    	}

    	datafiles.clear();

    	int file_num = 0;
    	for (final String filePath : files) {
    		File file = new File(filePath);
    		String fileName = file.getName();
    		int fileNameEndIndex = fileName.indexOf(ext);
    		if(fileNameEndIndex == -1){
    			continue;
    		}
    		
    		fileName = fileName.substring(0, fileNameEndIndex);
    		
    		num = Integer.parseInt(fileName);
    		
    		if (num < 1) {
    			continue;
    		}
    		DataFile dataFile = new DataFile();        	
    		dataFile.init(filePath);
    		
    		datafiles.put(num, dataFile);
    		if (num > cur_file_no) {
    			cur_file_no = num;
    		}
    		file_num++;
    	}
    	if (file_num > 0) {
    		cur_file_size = datafiles.get(cur_file_no).size();
    	}

    	if (cur_file_size >= kMaxFileSize) {
    		newFile();
    		file_num ++;
    	}
    	return file_num;
    }

    public final String no2FileName(final int no, final String ext) {
    	return dir + File.separator + BufferUtil.fillNo(no) + ext;
    }

	public void flush() {
		datafiles.get(cur_file_no).flush();
	}
}
