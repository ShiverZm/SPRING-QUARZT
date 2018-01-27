package org.dreams.fly.task;

import java.util.Random;

import org.dreams.fly.dao.domain.Example;
import org.dreams.fly.service.ExampleService;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author book
 * 此处不支持spring注解
 */
public class ColonyScheduleTask {

	private static final Logger LOG = LoggerFactory.getLogger(ColonyScheduleTask.class);
   
	@Autowired    
	private ExampleService exampleService;

	/** 
     * 调度创建表，方法中的参数是JobExecutionContext类型，要使CustomDetailQuartzJobBean中的executeInternal方法中利用反射机制调用到相应的方法 
     */  
    public void insertExample(JobExecutionContext context){
    	LOG.info("====> insertExample invoked start ");   
    	try {
            Example example = new Example();
            example.setName("张三"+new Random().nextInt(10000));
            boolean bo = exampleService.saveExample(example);
            LOG.info("====> 执行结果："+bo);
		} catch (Exception e) {
			LOG.error("insertExample 执行一次",e);
		}
    	LOG.info("====> insertExample invoked end "); 
    }  
      
    /** 
     * 调度任务栈，方法中的参数是JobExecutionContext类型，要使CustomDetailQuartzJobBean中的executeInternal方法中利用反射机制调用到相应的方法 
        */  
    public void secheduleTask(JobExecutionContext context){  
    	LOG.info ("====> secheduleTask invoked!");  
       
    }  
    /** 
     * 删除任务栈，方法中的参数是JobExecutionContext类型，要使CustomDetailQuartzJobBean中的executeInternal方法中利用反射机制调用到相应的方法 
     */  
    public void deleteTask(JobExecutionContext context){  
    	LOG.info("====> deleteTask invoked !"); 
    }  
	
}
