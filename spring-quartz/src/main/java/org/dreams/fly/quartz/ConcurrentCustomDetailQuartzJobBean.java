package org.dreams.fly.quartz;

import java.lang.reflect.Method;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;
/***
 * @author 艾国梁 
 */
public class ConcurrentCustomDetailQuartzJobBean extends QuartzJobBean {
	private static final Logger LOG = LoggerFactory.getLogger(ConcurrentCustomDetailQuartzJobBean.class);
	private String targetObject;    
    private String targetMethod;
    private ApplicationContext ctx;   
    @Override  
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException { 
     LOG.info("=====> execute [" + targetObject + "] at once");  
     try {    
            Object otargetObject = ctx.getBean(targetObject);    
            Method m = null;    
            try {  
                m = otargetObject.getClass().getMethod(targetMethod, new Class[] {JobExecutionContext.class}); //方法中的参数是JobExecutionContext类型  
                m.invoke(otargetObject, new Object[] {context});   
            } catch (SecurityException e) {    
               e.printStackTrace();  
            } catch (NoSuchMethodException e) {    
                e.printStackTrace();  
            }    
        } catch (Exception e) {    
            throw new JobExecutionException(e);    
        }    
    }  
    public void setApplicationContext(ApplicationContext applicationContext) {    
        this.ctx = applicationContext;    
    }    
    
    public void setTargetObject(String targetObject) {    
        this.targetObject = targetObject;    
    }    
    
    public void setTargetMethod(String targetMethod) {    
        this.targetMethod = targetMethod;    
    }
	

}
