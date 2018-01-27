package org.dreams.fly.quartz.listener;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/***
 * @author 艾国梁
 * Job监听
 */
public class CustomJobListener implements JobListener {

	private static final Logger LOG = LoggerFactory.getLogger(CustomJobListener.class);
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "CustomJobListener";
	}

	 /** 
     * Scheduler 在 JobDetail 即将被执行，但又被 TriggerListener 否决了时调用这个方法。 
     */  
	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		// TODO Auto-generated method stub
		LOG.info("====> jobToBeExecuted invoked.");

	}

	 /** 
     * Scheduler 在 JobDetail 将要被执行时调用这个方法。 
     */ 
	@Override
	public void jobExecutionVetoed(JobExecutionContext context) {
		// TODO Auto-generated method stub
		LOG.info("====> jobExecutionVetoed invoked.");

	}

	 /** 
     * Scheduler 在 JobDetail 被执行之后调用这个方法。 
     */  
	@Override
	public void jobWasExecuted(JobExecutionContext context,
			JobExecutionException jobException) {
		// TODO Auto-generated method stub
		LOG.info("====> jobWasExecuted invoked.");

	}

}
