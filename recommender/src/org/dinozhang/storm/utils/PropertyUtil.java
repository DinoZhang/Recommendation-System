package org.dinozhang.storm.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 属性配置读取工具
 * @author 
 */
public class PropertyUtil {

	private static final Log log = LogFactory.getLog(PropertyUtil.class);
	private static Properties pros = new Properties();
	
	/*//加载属性文件
	static {
		try {
			File configFile = new File("config.properties");
			FileInputStream inputStream = new FileInputStream(configFile);
			pros.load(inputStream);
		} catch (Exception e) {
			log.error("load configuration error", e);
	    }*/
		
		// 加载属性文件
		static {
			try {
				InputStream in = PropertyUtil.class.getClassLoader()
						.getResourceAsStream("config.properties");
				pros.load(in);
			} catch (Exception e) {
				log.error("load configuration error", e);
			}
	}
	
	/**
	 * 读取配置文中的属性值
	 * @param key
	 * @return
	 */
	public static String getProperty(String key){
		return pros.getProperty(key);
	}
	
}
