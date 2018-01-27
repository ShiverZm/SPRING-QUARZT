package org.dreams.fly.cache;


import org.dreams.fly.common.GlobalVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * 全局锁操作类
 */
public class DistributedLock {
	
	private final static Logger LOG = LoggerFactory.getLogger(DistributedLock.class);
	
	/** 线程可重入锁，用于实现同一个线程可重入全局锁 */
	private static volatile ThreadLocal<ConcurrentMap<String, Map<String, String>>> THREAD_CONTEXT = new ThreadLocal<ConcurrentMap<String, Map<String, String>>>(){
		
		@Override
		protected ConcurrentMap<String, Map<String, String>> initialValue() {
			ConcurrentMap<String, Map<String, String>> dict = new ConcurrentHashMap<>();
			dict.put(GlobalVariable.LOCK_CONSTANTS.REENTRANT_LOCK, new ConcurrentHashMap<String,String>());
			dict.put(GlobalVariable.LOCK_CONSTANTS.NO_REENTRANT_LOCK, new ConcurrentHashMap<String,String>());
			return dict;
		}
		
	};
	
	/**
	 * 获取一个非可重入的全局锁对象
     */
    public static void acquireLock(String namespace, String key, Long lockTimeToLive, Long acquireLockTimeout, TimeUnit timeUnit) {
    	
    	String fullKey = CacheOperator.getFullKey(namespace, key);
        // 取得最初的时间
        final long start = System.currentTimeMillis();
        
        Integer ttl = null;
        if(null != lockTimeToLive){
        	ttl = CacheOperator.getTimes(lockTimeToLive, timeUnit);
        }
        
        long acquireTime = Long.MAX_VALUE;
        if(null != acquireLockTimeout){
           acquireTime = TimeUnit.MILLISECONDS.convert(acquireLockTimeout, timeUnit);
        }
        final String value = UUID.randomUUID().toString();
        while (!CacheOperator.getOptString().setIfAbsent(fullKey, value.getBytes(), ttl)) {
            long cost = System.currentTimeMillis() - start;
            if (acquireTime <= cost) {
                // 获取锁超时
                throw new RuntimeException(new TimeoutException("Redis获取缓存namespace[" + namespace + "]和Key[" + key + "]超时超过" + acquireTime + "毫秒"));
            }
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                LOG.error("thread sleep occur error:", e);
            }
        }
        THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.NO_REENTRANT_LOCK).put(fullKey, value);
    }
    
    /**
     * 释放指定namespace下的key的锁
     */
    public static void releaseLock(String namespace, String key) {
    	final String fullKey = CacheOperator.getFullKey(namespace, key);
    	final String value = THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.NO_REENTRANT_LOCK).get(fullKey);
        if (null == value) {
            throw new RuntimeException(new TimeoutException(Thread.currentThread().getName()+"未获得锁["+key+"]无法释放锁"));
        }else{
        	String val = CacheOperator.getOptString().get(namespace, key);
        	if(value.equals(val)){
        		CacheOperator.getOptString().del(namespace, key);
        	}else if(null != val){
        		 throw new RuntimeException(Thread.currentThread().getName()+"未获得锁["+fullKey+"]无法释放锁");
        	}
        }
        THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.NO_REENTRANT_LOCK).remove(fullKey);
    }
	
	/**
     * 当成功获取该锁后，在未释放锁之前，再次调用此方法可以直接获取该锁，但设置的锁有效期和超时时间以第一次为准
     */
    public static void acquireReentrantLock(String namespace, String key, Long lockTimeToLive, Long acquireLockTimeout, TimeUnit timeUnit) {
    	
    	String fullKey = CacheOperator.getFullKey(namespace, key);
        
        // 可重入锁
    	if (THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.REENTRANT_LOCK).containsKey(fullKey)) {
            return;
        }

        // 取得最初的时间
        final long start = System.currentTimeMillis();
        
        Integer ttl = null;
        if(null != lockTimeToLive){
        	ttl = CacheOperator.getTimes(lockTimeToLive, timeUnit);
        }
        
        long acquireTime = Long.MAX_VALUE;
        if(null != acquireLockTimeout){
           acquireTime = TimeUnit.MILLISECONDS.convert(acquireLockTimeout, timeUnit);
        }
        final String value = UUID.randomUUID().toString();
        while (!CacheOperator.getOptString().setIfAbsent(fullKey, value.getBytes(), ttl)) {
            long cost = System.currentTimeMillis() - start;
            if (acquireTime <= cost) {
                // 获取锁超时
                throw new RuntimeException(new TimeoutException("Redis获取缓存namespace[" + namespace + "]和key[" + key + "]超时超过" + acquireTime + "毫秒"));
            }
            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                LOG.error("occur error:", e);
            }
        }
        THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.REENTRANT_LOCK).put(fullKey, value);
    }
    
    /**
     * 释放指定namespace下的key的锁
     */
    public static void releaseReentrantLock(String namespace, String key) {
    	final String fullKey = CacheOperator.getFullKey(namespace, key);
    	final String value = THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.REENTRANT_LOCK).get(fullKey);
        if (null == value) {
            throw new RuntimeException(new TimeoutException(Thread.currentThread().getName()+"未获得锁["+fullKey+"]无法释放锁"));
        }else{
        	String val = CacheOperator.getOptString().get(namespace, key);
        	if(value.equals(val)){
        		CacheOperator.getOptString().del(namespace, key);
        	}else if(null != val){
        		 throw new RuntimeException(Thread.currentThread().getName()+"未获得锁["+fullKey+"]无法释放锁");
        	}
        }
        THREAD_CONTEXT.get().get(GlobalVariable.LOCK_CONSTANTS.REENTRANT_LOCK).remove(fullKey);
    }

}