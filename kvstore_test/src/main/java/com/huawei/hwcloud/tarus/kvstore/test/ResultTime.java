package com.huawei.hwcloud.tarus.kvstore.test;

public enum ResultTime {
	
	INIT_TIME("init.time", "0"),
	UNINIT_TIME("uninit.time", "0"),
	WRITE_TIME("wrtie.time", "0"),
	READ_TIME("read.time", "0"),
	REINIT_TIME("reinit.time", "0"),
	REREAD_TIME("reread.time", "0"),
	READ_ERRORS("read.errors", "0"),
	REREAD_ERRORS("reread.errors", "0"),
	WRITE_ERRORS("write.errors", "0"),
	TOTAL_ERRORS("total.errors", "0"),
	
	TOTAL_TIME("total.time", "0"),
	
	FINAL_RESULT("final.result", "iternal error"),
	
	;
    
	 private final String timeName;
	 private final String defaultValue;
	 
	 private ResultTime(final String timeName, final String defaultValue){
	   this.timeName = timeName;
	   this.defaultValue = defaultValue;
	 }
	 
	 public final String getTimeName(){
	   return this.timeName;
	 }
	 
	 public final String getDefaultValue(){
	   return this.defaultValue;
	 }
}
