package org.dreams.fly.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class PropertiesUtils {
	private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtils.class);

	private static final Map<String, Map<String, String>> RESOURCES = new ConcurrentHashMap<String, Map<String, String>>();

	private static final Map<String, Properties> PROPERTIES = new ConcurrentHashMap<String, Properties>();

	private static final String DEFAULT_PROPERTIES_FILE = "default.properties";

	private static final String DEFAULT_FOLDER = "default";

	private static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().indexOf("win") >= 0;

	private PropertiesUtils() {

	}

	/**
	 * 从默认配置文件获取指定属性值
	 *
	 * @param property
	 *            属性名
	 * @return
	 */
	public static String getPropertiesValue(String property) {
		return getPropertiesValue(DEFAULT_PROPERTIES_FILE, property);
	}

	/**
	 * 以Map形式获取配置文件所有值对
	 * @param propertiesFile
	 * @return
	 */
	public static Map<String,String> getProperties(String propertiesFile) {
		Map<String,String> props= Collections.emptyMap();
		if (RESOURCES.containsKey(propertiesFile)) {
			props = RESOURCES.get(propertiesFile);
		}else {
			props=loadPropertyFile(propertiesFile);
		}
		return props;
	}

	/**
	 * 根据配置文件获取指定key值
	 * @param propertiesFile 属性文件
	 * @param property 属性名
	 * @return
	 */
	public static String getPropertiesValue(String propertiesFile, String property) {
		String val = "";

		//先从resource bundle中取
		if (RESOURCES.containsKey(propertiesFile)) {
			val = RESOURCES.get(propertiesFile).get(property);
		} else {
			Map<String,String> res = loadPropertyFile(propertiesFile);
			if (null!=res) {
				val = res.get(property);
			}
		}

		//取不到从Props里面取
		if("".equals(val) && PROPERTIES.containsKey(propertiesFile)) {
			return PROPERTIES.get(propertiesFile).getProperty(property);
		}

		return val;
	}

	private static Map<String, String> loadPropertyFile(String propertiesFile) {
		try {
			String props = propertiesFile;
			ClassLoader classLoader = PropertiesUtils.class.getClassLoader();
			if(IS_WINDOWS && DEFAULT_PROPERTIES_FILE.equalsIgnoreCase(propertiesFile)){
				if(null == classLoader.getResource(propertiesFile)){
					props =  DEFAULT_FOLDER + "/" + DEFAULT_PROPERTIES_FILE;
				}
			}else if(DEFAULT_PROPERTIES_FILE.equals(propertiesFile)){
				if(null == classLoader.getResource(propertiesFile)){
					props =  DEFAULT_FOLDER + "/" + DEFAULT_PROPERTIES_FILE;
				}
			}
			Map<String, String> result = new HashMap<String, String>();
			result = PropertiesLoader.loadContextProps(props);
			RESOURCES.put(propertiesFile, result);
			return result;
		} catch (Exception e) {
			LOG.error("occur error:", e);
		}
		return null;
	}

	/**
	 * 添加配置props
	 * @param propKey
	 * @param props
	 */
	public static void put(String propKey,Properties props) {
		PROPERTIES.put(propKey,props);
	}

	public static boolean isWindows() {
		return IS_WINDOWS;
	}


}
