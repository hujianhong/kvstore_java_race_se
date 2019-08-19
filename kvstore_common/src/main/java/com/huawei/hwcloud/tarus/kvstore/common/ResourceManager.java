package com.huawei.hwcloud.tarus.kvstore.common;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import com.huawei.hwcloud.tarus.kvstore.exception.KVSErrorCode;
import com.huawei.hwcloud.tarus.kvstore.exception.KVSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManager {

	private static final Logger log = LoggerFactory.getLogger(ResourceManager.class);
	
	private static final Properties config = new Properties();
	
	private static String home="/Users/jianhong/work/huawei";
	
	private static int threadNum=0;
	
	private static int kvNumPerThread=0;
	
	private static String executeMode= "";
	
	private static String checkModeParam= "";

	private static String rpcUriParam= "";

	private static String clearDataFlagParam= "";
	
	private static CheckMode checkMode = null;

	private static RPCUri rpcUri = null;

	private static boolean clearDataFlag= true;
	
	public static final void init(String[] params) {
		initConfig(params);
		parseParams(params);
		validateParams();
		loadCheckMode();
		loadExecuteMode();
		loadRpcUri();
		loadClearDataFlag();
	}

	private static final void initConfig(String[] params) {

		Configuration[] configurations = Configuration.values();
		for(int i=0; i<configurations.length; i++){
			Configuration configuration= configurations[i];
			String value= ConfigManager.getConfigByDefalt(configuration);
			config.put(configuration.getParamName(), value);
		}
		
		System.out.println("system config=[" + config + "]");
	}
	
	private static final void parseParams(String[] params) {
		parseHome(params);
		parseThreadNum();
		parseKVNumPerThread();
		parseExecuteMode();
		parseCheckMode();
		parseRpcUri();
		parseClearDataFlag();
	}
	
	private static final void validateParams() {
		if (StringUtils.isEmpty(executeMode) && StringUtils.isEmpty(checkModeParam)) {
			throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR, 
					String.format("param:[%s && %s] are both null!", 
							Configuration.KVS_CONFIG__KVS_EXECUTE_MODE.getParamName(), 
							Configuration.KVS_CONFIG__KVS_CHECK_MODE.getParamName()));
		}
	}

	private static final void parseHome(String[] params) {
		
		String tmpKVHome = System.getProperty(Configuration.KVS_CONFIG_HOME.getParamName());
		
		String userdir = config.getProperty(Configuration.KVS_CONFIG_USER_DIR.getParamName());
		
		if (StringUtils.isEmpty(tmpKVHome)) {
			tmpKVHome = userdir;
			File userdirfile = new File(userdir);
			if (userdir.lastIndexOf("/bin/") != -1
					|| userdir.lastIndexOf("/bin") != -1) {
				userdirfile = userdirfile.getParentFile();
				tmpKVHome = userdirfile.getAbsolutePath();
			}
		} else {
			try {
				tmpKVHome = new File(tmpKVHome).getCanonicalPath();
			} catch (IOException e) {
				throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR, 
						printParamNull(Configuration.KVS_CONFIG_USER_DIR),
						e);
			}
		}
		home= tmpKVHome;
		System.out.println("Home=[" + home + "]");
	}
	
	private static final String printParamNull(Configuration configuration){
		return String.format("param:[%s] is null!", configuration.getParamName());
	}

	private static final void parseThreadNum() {
		String threadNumStr = config.getProperty(Configuration.KVS_CONFIG__KVS_THREAD_NUM.getParamName());
		if (StringUtils.isNotEmpty(threadNumStr)) {
			threadNum= Integer.parseInt(threadNumStr);
		}
	}
	
	private static final void parseKVNumPerThread() {
		String kvNumPerThreadStr = config.getProperty(Configuration.KVS_CONFIG__KVS_PER_THREAD_KV_NUM.getParamName());
		if (StringUtils.isNotEmpty(kvNumPerThreadStr)) {
			kvNumPerThread= Integer.parseInt(kvNumPerThreadStr);
		}
	}
	
	private static final void parseExecuteMode() {
		executeMode = config.getProperty(Configuration.KVS_CONFIG__KVS_EXECUTE_MODE.getParamName());
	}
	
	private static final void parseCheckMode() {
		checkModeParam = config.getProperty(Configuration.KVS_CONFIG__KVS_CHECK_MODE.getParamName());
	}

	private static final void parseRpcUri() {
		rpcUriParam = config.getProperty(Configuration.KVS_CONFIG_KVS_RPC_URL.getParamName());
	}

	private static final void parseClearDataFlag() {
		clearDataFlagParam = config.getProperty(Configuration.KVS_CONFIG_KVS_CLEAR_DATA.getParamName());
	}
	
	/**
	 * get param
	 */
	
	public static final String getHome() {
		return home;
	}

	public static final int getThreadNum() {
		return threadNum;
	}

	public static final int getKvNumPerThread() {
		return kvNumPerThread;
	}

	public static final String getExecuteMode() {
		return executeMode;
	}

	public static final String getCheckModeParam() {
		return checkModeParam;
	}
	
	public static final CheckMode getCheckMode() {
		return checkMode;
	}

	public static final String getRpcUriParam(){
		return rpcUriParam;
	}

	public static final RPCUri getRpcUri(){
		return rpcUri;
	}

	public static final  String getClearDataFlagParam(){
		return clearDataFlagParam;
	}

	public static final boolean getClearDataFlag(){
		return clearDataFlag;
	}
	
	private static final void loadExecuteMode() {
		String tmpExecuteMode = getExecuteMode();
		if (StringUtils.isNotEmpty(tmpExecuteMode)) {

			if(checkMode == CheckMode.KVSTORE){
				tmpExecuteMode = CheckMode.KVSTORE.getModeName() + StringValue.Underscore + tmpExecuteMode;
			}else{
				tmpExecuteMode = CheckMode.KVSERVICE.getModeName() + StringValue.Underscore + tmpExecuteMode;
			}

			log.info("execute mode:[{}]", tmpExecuteMode);

			try {
				ExecuteMode mode = ExecuteMode.valueOf(tmpExecuteMode.toUpperCase());
				if(checkMode != CheckMode.KVSTORE){
					KVStoreRace race = (KVStoreRace) Class.forName(mode.getModeClazz()).newInstance();
					RaceManager.instance().registerRacer(race);
				}else{
					KVStoreCheck check = (KVStoreCheck) Class.forName(mode.getModeClazz()).newInstance();
					RaceManager.instance().registerChecker(check);
				}
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new KVSException(KVSErrorCode.UNSUPPORTED_MODE_ERROR,
						String.format("unsupported execute mode:[%s] for racer!", tmpExecuteMode), e);
			}
		}else{
			throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR,
					String.format("execute mode:[%s] is null!", tmpExecuteMode));
		}
	}

	private static final void loadCheckMode() {
		if (StringUtils.isNotEmpty(getCheckModeParam())) {

			try {
				checkMode = CheckMode.valueOf(getCheckModeParam().toUpperCase());
				if(checkMode == CheckMode.KVSTORE){
					return;
				}
				KVStoreCheck check = (KVStoreCheck) Class.forName(getCheckMode().getModeClazz()).newInstance();
				RaceManager.instance().registerChecker(check);
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new KVSException(KVSErrorCode.UNSUPPORTED_MODE_ERROR, 
						String.format("unsupported check mode:[%s] for racer!", getCheckModeParam()));
			}
		}
	}
	
	public static final KVStoreRace initExecute() {
		KVStoreRace race = null;
		String tmpExecuteMode = getExecuteMode();
		if (StringUtils.isNotEmpty(tmpExecuteMode)) {

			if(checkMode == CheckMode.KVSTORE){
				tmpExecuteMode = CheckMode.KVSTORE.getModeName() + StringValue.Underscore + tmpExecuteMode;
			}else{
				tmpExecuteMode = CheckMode.KVSERVICE.getModeName() + StringValue.Underscore + tmpExecuteMode;
			}

			try {
				ExecuteMode mode = ExecuteMode.valueOf(tmpExecuteMode.toUpperCase());
				if(checkMode != CheckMode.KVSTORE){
					race = (KVStoreRace) Class.forName(mode.getModeClazz()).newInstance();
				}else{
					throw new KVSException(KVSErrorCode.UNSUPPORTED_MODE_ERROR,
							String.format("unsupported execute mode:[%s] for racer!", tmpExecuteMode));
				}
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new KVSException(KVSErrorCode.UNSUPPORTED_MODE_ERROR, 
						String.format("unsupported execute mode:[%s] for racer!", tmpExecuteMode));
			}
		}else{
			throw new KVSException(KVSErrorCode.CONFIG_PARAM_ERROR, 
					String.format("execute mode:[%s] is null!", tmpExecuteMode));
		}
		Validate.notNull(race, "failed to init race=[" + tmpExecuteMode + "] init!");
		return race;
	}

	private static final void loadRpcUri() {
		if(rpcUri == null){
			rpcUri = new RPCUri(getRpcUriParam());
		}
	}

	private static final void loadClearDataFlag(){
		if(getClearDataFlagParam().equalsIgnoreCase(StringValue.FALSE_FALG)){
			clearDataFlag= false;
		}else{
			clearDataFlag= true;
		}
	}
	
	public static final String buildFullDir(final String dir){
		return new StringBuilder().append(getHome()).append(File.separator).append(dir).toString();
	}
}
