package org.dreams.fly.common;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 艾国梁
 * java 线程池
 */
public class CorbetAgent {

	private static final Logger LOG = LoggerFactory.getLogger(CorbetAgent.class);

	public static abstract class Task implements Runnable{

		private final boolean isTrace;

		public Task(){
			this(false);
		}

		public Task(boolean isTrace){
			this.isTrace = isTrace;
		}

		@Override
		public abstract void run();

		public boolean isTrace() {
			return isTrace;
		}

	}

	static class DefaultExceptionHandler implements UncaughtExceptionHandler{

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			LOG.error("thread [thread name:" + t.getName(), e);
		}

	}

	static class CallerRunsPolicy implements RejectedExecutionHandler {

        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (!e.isShutdown()) {
            	LOG.warn("thread pool occupy completely!!!!");
                r.run();
            }
        }
    }

	static final ExecutorService EXECUTOR = new ThreadPoolExecutor(GlobalVariable.CORE_POOL, GlobalVariable.MAX_NUM_POOL,
			GlobalVariable.KEEP_ALIVE_TIME, TimeUnit.MINUTES,
            new SynchronousQueue<Runnable>(), new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r);
					t.setUncaughtExceptionHandler(new DefaultExceptionHandler());
					t.setPriority(6);
					t.setDaemon(true);
					return t;
				}
			}, new CallerRunsPolicy()){

				@Override
				protected void afterExecute(Runnable r, Throwable t) {
					if(r instanceof Task){
						Task task = (Task) r;
						if(task.isTrace){
							printException(r, t);
						}
					}else{
						printException(r, t);
					}
				}

				private void printException(Runnable r, Throwable t) {
				    if (t == null && r instanceof Future<?>) {
				        try {
				            Future<?> future = (Future<?>) r;
				            if (future.isDone()){
				                future.get();
				            }else{
				            	future.get(5, TimeUnit.MINUTES);
				            }
				        } catch (CancellationException e) {
				            t = e;
				        } catch (ExecutionException e) {
				            t = e.getCause();
				        } catch (InterruptedException ie) {
				            Thread.currentThread().interrupt();
				        } catch (TimeoutException e) {
				            t = e;
						}
				    }
				    if (t != null){
				    	LOG.error(t.getMessage(), t);
				    }
				}

	};

	/**
	 * 提交异步任务但不需要知晓和处理执行结果
	 */
	public static void doIt(Task task){
		EXECUTOR.submit(task);
	}

	/**
	 * 提交异步任务，同时需要得到程序的处理结果
	 */
	public static Future<?> getIt(Task task){
		return EXECUTOR.submit(task);
	}

}
