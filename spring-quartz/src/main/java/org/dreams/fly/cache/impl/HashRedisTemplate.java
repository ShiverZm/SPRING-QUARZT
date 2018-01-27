package org.dreams.fly.cache.impl;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

public class HashRedisTemplate extends KeyRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(HashRedisTemplate.class);

	public boolean set(String key, byte[] field, byte[] value, Integer expireInSecs) {
		return set(null, key, field, value, expireInSecs, true);
	}

	/**
	 * hash 的set方法，当expireInSecs为null或者小于零时，即不设置过期时间
	 */
	public boolean set(Integer db, String key, byte[] field, byte[] value, Integer expireInSecs,
			boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (expireInSecs != null && expireInSecs > 0) {
				Pipeline pipeline = jedis.pipelined();
				Response<Long> setResponse = pipeline.hset(key.getBytes("UTF-8"), field, value);
				Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
				pipeline.sync();
				if (setResponse.get() != null && setResponse.get() >= 0) {
					if (expireResponse.get() != null && expireResponse.get() == 1) {
						result = true;
					}
				}
			} else {
				Long response = jedis.hset(key.getBytes("UTF-8"), field, value);
				if (response != null && response >= 0) {
					result = true;
				}
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public boolean setIfAbsent(String key, byte[] field, byte[] value, Integer expireInSecs) {
		return setIfAbsent(null, key, field, value, expireInSecs, true);
	}

	/**
	 * 仅当key不存在时才会保存该值，当expireInSecs为null或者小于零时，即不设置过期时间
	 */
	public boolean setIfAbsent(Integer db, String key, byte[] field, byte[] value, Integer expireInSecs,
			boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (expireInSecs != null && expireInSecs > 0) {
				Pipeline pipeline = jedis.pipelined();
				Response<Long> setResponse = pipeline.hsetnx(key.getBytes("UTF-8"), field, value);
				Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
				pipeline.sync();
				if (setResponse.get() != null && setResponse.get() >= 0) {
					if (expireResponse.get() != null && expireResponse.get() == 1) {
						result = true;
					}
				}
			} else {
				Long response = jedis.hsetnx(key.getBytes("UTF-8"), field, value);
				if (response != null && response >= 0) {
					result = true;
				}
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public boolean muiltSet(String key, Map<byte[], byte[]> kvs, Integer expireInSecs) {
		return muiltSet(null, key, kvs, expireInSecs, true);
	}

	/**
	 * 同时保存多个数值，当expireInSecs为null或者小于零时，即不设置过期时间
	 */
	public boolean muiltSet(Integer db, String key, Map<byte[], byte[]> kvs, Integer expireInSecs,
			boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (expireInSecs != null && expireInSecs > 0) {
				Pipeline pipeline = jedis.pipelined();
				Response<String> setResponse = pipeline.hmset(key.getBytes("UTF-8"), kvs);
				Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
				pipeline.sync();
				if (OK.equalsIgnoreCase(setResponse.get())) {
					if (expireResponse.get() != null && expireResponse.get() == 1) {
						result = true;
					}
				}
			} else {
				String response = jedis.hmset(key.getBytes("UTF-8"), kvs);
				if (OK.equalsIgnoreCase(response)) {
					result = true;
				}
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public byte[] get(String key, byte[] field) {
		return get(null, key, field, true);
	}

	/**
	 * 获取指定的key的field字段
	 * 
	 * @param db
	 * @param key
	 * @param field
	 * @param isSkipSelect
	 * @return
	 */
	public byte[] get(Integer db, String key, byte[] field, boolean isSkipSelect) {
		Jedis jedis = null;
		byte[] result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.hget(key.getBytes("UTF-8"), field);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public Map<byte[], byte[]> muiltGet(String key, byte[][] fields) {
		return muiltGet(null, key, fields, true);
	}

	/**
	 * 获取指定的key的多个field字段
	 */
	public Map<byte[], byte[]> muiltGet(Integer db, String key, byte[][] fields, boolean isSkipSelect) {
		Jedis jedis = null;
		Map<byte[], byte[]> result = new HashMap<byte[], byte[]>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			List<byte[]> response = jedis.hmget(key.getBytes("UTF-8"), fields);
			if (CollectionUtils.isNotEmpty(response)) {
				for (int i = 0; i < fields.length; i++) {
					if (null != response.get(i)) {
						result.put(fields[i], response.get(i));
					}
				}
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public Map<byte[], byte[]> getAll(String key) {
		return getAll(null, key, true);
	}

	/**
	 * 获取指定的key的所有field字段
	 */
	public Map<byte[], byte[]> getAll(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		Map<byte[], byte[]> result = Collections.emptyMap();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Map<byte[], byte[]> response = jedis.hgetAll(key.getBytes("UTF-8"));
			if (MapUtils.isNotEmpty(response)) {
				result = response;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public boolean rm(String key, String fields) {
		return rm(null, key, fields, true);
	}

	public boolean rm(Integer db, String key, String fields, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long response = jedis.hdel(key, fields);
			if (null != response && response > 0) {
				result = true;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public boolean rm(String key, byte[][] fields) {
		return rm(null, key, fields, true);
	}

	/**
	 * 获取指定的key的特定field字段
	 */
	public boolean rm(Integer db, String key, byte[][] fields, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long response = jedis.hdel(key.getBytes("UTF-8"), fields);
			if (null != response && response > 0) {
				result = true;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public boolean rmAll(String key) {
		return rmAll(null, key, true);
	}

	/**
	 * 获取指定的key
	 */
	public boolean rmAll(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long response = jedis.del(key.getBytes("UTF-8"));
			if (null != response && response > 0) {
				result = true;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public Set<byte[]> fields(String key) {
		return fields(null, key, true);
	}

	/**
	 * 获取指定的key的所有Fields
	 */
	public Set<byte[]> fields(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		Set<byte[]> result = Collections.emptySet();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> response = jedis.hkeys(key.getBytes("UTF-8"));
			if (null != response) {
				result = response;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public List<byte[]> values(Integer db, String key) {
		return values(null, key, true);
	}

	/**
	 * 获取指定的key的所有Field的values
	 */
	public List<byte[]> values(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		List<byte[]> result = Collections.emptyList();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			List<byte[]> response = jedis.hvals(key.getBytes("UTF-8"));
			if (null != response) {
				result = response;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public Long incrBy(String key, byte[] field, long deta, Integer expireInSecs) {
		return incrBy(null, key, field, deta, expireInSecs, true);
	}

	public Long incrBy(Integer db, String key, byte[] field, long deta, Integer expireInSecs,
			boolean isSkipSelect) {
		Jedis jedis = null;
		Long result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (deta != 0) {
				if (null != expireInSecs) {
					Pipeline pipeline = jedis.pipelined();
					Response<Long> incrResponse = pipeline.hincrBy(key.getBytes("UTF-8"), field, deta);
					Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
					pipeline.sync();
					if (expireResponse != null && expireResponse.get() > 0) {
						if (null != incrResponse) {
							result = incrResponse.get();
						}
					}
				} else {
					result = jedis.hincrBy(key.getBytes("UTF-8"), field, deta);
				}
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public Double incrByFloat(String key, byte[] field, double deta, Integer expireInSecs) {
		return incrByFloat(null, key, field, deta, expireInSecs, true);
	}

	public Double incrByFloat(Integer db, String key, byte[] field, double deta, Integer expireInSecs,
			boolean isSkipSelect) {
		Jedis jedis = null;
		Double result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (deta != 0.0) {
				if (null != expireInSecs) {
					Pipeline pipeline = jedis.pipelined();
					Response<Double> incrResponse = pipeline.hincrByFloat(key.getBytes("UTF-8"), field, deta);
					Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
					pipeline.sync();

					if (expireResponse != null && expireResponse.get() > 0) {
						if (null != incrResponse) {
							result = incrResponse.get();
						}
					}
				} else {
					result = jedis.hincrByFloat(key.getBytes("UTF-8"), field, deta);
				}
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

}
