package org.dreams.fly.cache.impl;

import redis.clients.jedis.Jedis;

public interface RedisTemplate {

	public interface JedisAction<T> {
		T action(Jedis jedis);
	}

	public static interface Hook<T> {
		public boolean onResult(T param);
	}

}
