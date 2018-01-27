package org.dreams.fly.common;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 13-9-10
 * Time: 上午9:27
 */
public class PropertiesLoader {

	private static Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);
    
    public static Map<String, String> loadContextProps(String propertyFile){
    	Map<String, String> result = new HashMap<String, String>();
    	InputStream in = null;
    	try {
			Properties props = new Properties();
			in = PropertiesLoader.class.getClassLoader().getResourceAsStream(propertyFile);
			props.load(in);
			
			Enumeration<Object> keys = props.keys();
			
		    while(keys.hasMoreElements()){
		    	String key = (String)keys.nextElement();
		    	result.put(key, props.getProperty(key));
		    }
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally{
			try {
				if(null!=in) in.close();
			} catch (IOException e) {
				LOG.error("close io stream occur error:", e);
			}
		}
    	return result;
    }
    
    public static Map<String, String> loadProps(File file){
    	Map<String, String> result = new HashMap<String, String>();
    	FileInputStream fis = null;
    	try {
			Properties props = new Properties();
			
			fis = new FileInputStream(file);
			
			props.load(fis);
			
			Enumeration<Object> keys = props.keys();
			
		    while(keys.hasMoreElements()){
		    	String key = (String)keys.nextElement();
		    	result.put(key, props.getProperty(key));
		    }
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally{
			if(null != fis){
				try {
					fis.close();
				} catch (IOException e) {
					LOG.error("occur error:", e);
				}
			}
		}
    	return result;
    }
    
    public static String loadProps(String propertyFile, String propertyName){
    	Map<String, String> result = loadContextProps(propertyFile);
    	return result.get(propertyName);
    }
    
}
