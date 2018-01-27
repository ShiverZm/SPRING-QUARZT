package org.dreams.fly.cache.impl;


import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dreams.fly.common.PropertiesUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

public class AbsRedisTemplate implements RedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(AbsRedisTemplate.class);

	protected static final Long SUCCESS = 1L;
	protected static final String OK = "OK";
	protected static byte[] KEY_NOT_EXIST = "NX".getBytes();
	protected static byte[] KEY_EXISTS = "XX".getBytes();

	public static final JedisPool JEDIS_POOL;
	
	protected static final int DEFAULT_DATABASE = 0;
	
	private static final String CONFIG_FILE = "redis.properties"; 

	static {
		JedisPoolConfig config = new JedisPoolConfig();
		
		Map<String, String> props = PropertiesUtils.getProperties(CONFIG_FILE);
		
		// 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
		// 如果赋值为-1，则表示不限制；如果pool已经分配了MaxTotal个jedis实例，则此时pool的状态为exhausted(耗尽)。
		config.setMaxTotal(Integer.parseInt(props.get("redis.pool.maxTotal")));
		
		// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
		config.setMaxIdle(Integer.parseInt(props.get("redis.pool.maxIdle")));
		// 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
		config.setMaxWaitMillis(Long.parseLong(props.get("redis.pool.maxWaitMillis")));
		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		config.setTestOnBorrow(true);
		
		String password = props.get("redis.auth.password");
		
		JEDIS_POOL = new JedisPool(config, props.get("redis.pool.host"),
				Integer.parseInt(props.get("redis.pool.port")), Protocol.DEFAULT_TIMEOUT, StringUtils.isEmpty(password) ? null : password, DEFAULT_DATABASE);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				JEDIS_POOL.destroy();
			}
		});
	}

	protected void selectIndex(Jedis jedis, Integer index) {
		String response = null;
		if (index == null) {
			response = jedis.select(DEFAULT_DATABASE);
		} else if (index >= 0 && index < Byte.MAX_VALUE) {
			response = jedis.select(index);
		} else {
			LOG.info("redis select index occur error [idenx=" + index + "]");
			throw new RuntimeException("redis select index occur error [idenx=" + index + "]");
		}

		if (!OK.equalsIgnoreCase(response)) {
			LOG.info("redis select index occur error [idenx=" + index + "]");
			throw new RuntimeException("redis select index occur error [idenx=" + index + "]");
		}
	}

	public final <T> T execute(JedisAction<T> jedisAction, Integer db, boolean isSkipSelect) throws JedisException {
		Jedis jedis = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if(!isSkipSelect){
				selectIndex(jedis, db);
			}
			return jedisAction.action(jedis);
		} catch (JedisException e) {
			handleJedisException(e);
			throw e;
		} finally {
			selectIndex(jedis, DEFAULT_DATABASE);
			closeResource(jedis);
		}
	}
	
	public final <T> T execute(JedisAction<T> jedisAction) throws JedisException {
		Jedis jedis = null;
		try {
			jedis = JEDIS_POOL.getResource();
			selectIndex(jedis, DEFAULT_DATABASE);
			return jedisAction.action(jedis);
		} catch (JedisException e) {
			handleJedisException(e);
			throw e;
		} finally {
			selectIndex(jedis, DEFAULT_DATABASE);
			closeResource(jedis);
		}
	}

	protected boolean handleJedisException(Exception exception) {
		if (exception instanceof JedisConnectionException) {
			LOG.error("redis connection lost :", exception);
		} else if (exception instanceof JedisDataException) {
			if ((exception.getMessage() != null) && (exception.getMessage().indexOf("READONLY") != -1)) {
				LOG.error("redis connection are read-only slave :", exception);
			} else {
				LOG.error("jedis exception happen:", exception);
				return false;
			}
		} else {
			LOG.error("jedis exception happen:", exception);
		}
		return true;
	}

	protected void closeResource(Jedis jedis) {
		try {
			if (null != jedis) {
				jedis.close();
			}
		} catch (Exception e) {
			LOG.error("return back jedis failed, will fore close the jedis:", e);
			destroyJedis(jedis);
		}

	}

	protected void destroyJedis(Jedis jedis) {
		if ((jedis != null) && jedis.isConnected()) {
			try {
				try {
					jedis.quit();
				} catch (Exception e) {
					LOG.error("jedis quit occur error:", e);
				}
				jedis.disconnect();
			} catch (Exception e) {
				LOG.error("jedis disconnect occur error:", e);
			}
		}
	}

}
