package org.dreams.fly.quartz.listener;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/***
 * @author 艾国梁
 * trigger监听
 */
public class CustomTriggerListener implements TriggerListener {

	private static final Logger LOG = LoggerFactory.getLogger(CustomTriggerListener.class);
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "CustomTriggerListener";
	}

	/***
	 * (1)
	 * Trigger被激发 它关联的job即将被运行 
	 */
	@Override
	public void triggerFired(Trigger trigger, JobExecutionContext context) {
		// TODO Auto-generated method stub
		LOG.info("====> triggerFired");
	}

	/**
	 * (2)
	 * Trigger被激发 它关联的job即将被运行,先执行(1)，在执行(2) 如果返回TRUE 那么任务job会被终止
	 */
	@Override
	public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * (3)
	 * 当Trigger错过被激发时执行,比如当前时间有很多触发器都需要执行，但是线程池中的有效线程都在工作， 
     *  那么有的触发器就有可能超时，错过这一轮的触发。 
	 */
	@Override
	public void triggerMisfired(Trigger trigger) {
		// TODO Auto-generated method stub
        LOG.info("=======> triggerMisfired:"+trigger.getCalendarName());
	}

	/**
	 * (4) 
	 * 任务完成时触发 
	 */
	@Override
	public void triggerComplete(Trigger trigger, JobExecutionContext context,
			CompletedExecutionInstruction triggerInstructionCode) {
		// TODO Auto-generated method stub

	}

}
