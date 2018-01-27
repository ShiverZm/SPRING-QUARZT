package org.dreams.fly.quartz.listener;

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.SchedulerListener;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/***
 * @author 艾国梁
 * scheduler监听
 */
public class CustomSchedulerListener implements SchedulerListener {

	private static final Logger LOG = LoggerFactory.getLogger(CustomSchedulerListener.class);
	
	/**
	 * Scheduler 在有新的 JobDetail 部署时调用此方法。
	 */
	@Override
	public void jobScheduled(Trigger trigger) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobScheduled invoked.");
		
	}

	/**
	 * Scheduler 在有新的 JobDetail卸载时调用此方法
	 */
	@Override
	public void jobUnscheduled(TriggerKey triggerKey) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobUnscheduled invoked.");
		
	}

	/**
	 * 当一个 Trigger 来到了再也不会触发的状态时调用这个方法。除非这个 Job 已设置成了持久性，否则它就会从 Scheduler 中移除。
	 */
	@Override
	public void triggerFinalized(Trigger trigger) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener triggerFinalized invoked.");
		
	}

	/**
	 * 	Scheduler 调用这个方法是发生在一个 Trigger 或 Trigger 组被暂停时。假如是 Trigger 组的话，triggerName 参数将为 null。
	 */
	@Override
	public void triggerPaused(TriggerKey triggerKey) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener triggerPaused invoked.");
		
	}

	@Override
	public void triggersPaused(String triggerGroup) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener triggersPaused invoked.");
		
	}

	/**
	 * Scheduler 调用这个方法是发生成一个 Trigger 或 Trigger 组从暂停中恢复时。假如是 Trigger 组的话，triggerName 参数将为 null
	 */
	@Override
	public void triggerResumed(TriggerKey triggerKey) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener triggerResumed  triggerKey invoked.");
		
	}
    /**
     * 当一个或一组 JobDetail 暂停时调用这个方法。
     */
	@Override
	public void triggersResumed(String triggerGroup) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener triggersResumed triggerGroup  invoked.");
		
	}
    
    /**
     * 当添加一个 job 是执行
     */
	@Override
	public void jobAdded(JobDetail jobDetail) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobAdded invoked.");
		
	}

	/**
	 * 当添删除一个 job时执行
	 */
	@Override
	public void jobDeleted(JobKey jobKey) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobDeleted invoked.");
		
	}

	@Override
	public void jobPaused(JobKey jobKey) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobPaused jobKey invoked.");
		
	}

	@Override
	public void jobsPaused(String jobGroup) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobPaused jobGroup invoked.");
		
	}

	@Override
	public void jobResumed(JobKey jobKey) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobResumed jobKey invoked.");
		
	}

	@Override
	public void jobsResumed(String jobGroup) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener jobResumed jobGroup invoked.");
		
	}

	@Override
	public void schedulerError(String msg, SchedulerException cause) {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulerError invoked.");
		
	}

	@Override
	public void schedulerInStandbyMode() {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulerInStandbyMode invoked.");
		
	}

	@Override
	public void schedulerStarted() {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulerStarted invoked.");
		
	}

	@Override
	public void schedulerStarting() {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulerStarting invoked.");
		
	}

	@Override
	public void schedulerShutdown() {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulerShutdown invoked.");
		
	}

	@Override
	public void schedulerShuttingdown() {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulerShuttingdown invoked.");
		
	}

	@Override
	public void schedulingDataCleared() {
		// TODO Auto-generated method stub
		LOG.info("==========> customSchedulerListener schedulingDataCleared invoked.");
		
	}

	

}
