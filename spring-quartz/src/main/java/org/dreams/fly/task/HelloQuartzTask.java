package org.dreams.fly.task;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author 艾国梁 
 */
public class HelloQuartzTask {
	
	private static final Logger LOG = LoggerFactory.getLogger(HelloQuartzTask.class);

	public void work() {  
		LOG.info("hello simple quartz:" + new Date());      
    }  
	
	
}
