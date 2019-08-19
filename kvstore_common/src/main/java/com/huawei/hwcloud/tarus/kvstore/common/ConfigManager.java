package com.huawei.hwcloud.tarus.kvstore.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

public class ConfigManager {
    
    public static final String COMMON_CONFIG_EXCEPTION = "exception";

    public static final String META_FILE_SUFFIX = ".meta";

    public static final String DATA_FILE_SUFFIX = ".data";
    
    public static final String DATA_FILE_DIR = "data";

    public static final byte KV_OP_DATA_APPEND = 0x00;
    public static final byte KV_OP_DATA_GET = 0x01;
    public static final byte KV_OP_META_APPEND = 0x02;
    public static final byte KV_OP_META_GET = 0x03;
    public static final byte KV_OP_CLEAR = 0x04;
    
    private final static String getConfig(final String key){
        return System.getProperty(key);
    }
    
    public final static String getConfigByDefalt(final Configuration configName){
    	return getConfigByDefalt("", configName);
    }
    
    public final static String getConfigByDefalt(final String prefix, final Configuration configName){
        final String key =  prefix + configName.getParamName();
        String config = getConfig(key);
        if (StringUtils.isEmpty(config)) {
            config = configName.getDefaultValue();
            if (StringUtils.isNotEmpty(config) && config.equals(ConfigManager.COMMON_CONFIG_EXCEPTION)) {
                Validate.isTrue(StringUtils.isNotEmpty(config), "empty value for config property=[" + key + "]");
            }
        }
        return config;
    }
    
    public final static void setConfig(final Configuration configName, final String value){
    	System.setProperty(configName.getParamName(), value);
    }
}
