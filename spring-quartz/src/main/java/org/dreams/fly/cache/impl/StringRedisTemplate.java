package org.dreams.fly.cache.impl;


import org.dreams.fly.common.Pair;
import org.dreams.fly.common.Tuple;
import org.dreams.fly.common.bytes.Bytes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringRedisTemplate extends KeyRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(StringRedisTemplate.class);
	
	
	public boolean setIfAbsent(String key, byte[] value, Integer expireInSecs) {
		return setIfAbsent(null, key, value, expireInSecs, true);
	}

	public boolean setIfAbsent(Integer db, String key, byte[] value, Integer expireInSecs, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Pipeline pipeline = jedis.pipelined();
			Response<String> setResponse = pipeline.set(key.getBytes("UTF-8"), value, KEY_NOT_EXIST);
			Response<Long> expireResponse = null;
			if (expireInSecs != null && expireInSecs > 0) {
				expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
			}
			pipeline.sync();
			if (OK.equalsIgnoreCase(setResponse.get())) {
				result = SUCCESS.equals(expireResponse.get());
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

	public boolean setIfAbsentInTransaction(List<Tuple<String, byte[], Integer>> tupleList) {
		return setIfAbsentInTransaction(null, tupleList, true);
	}

	public boolean setIfAbsentInTransaction(Integer db, List<Tuple<String, byte[], Integer>> tupleList,
			boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] keys = new byte[tupleList.size()][];
			byte[][] keyvalues = new byte[tupleList.size() * 2][];
			List<Pair<byte[], Integer>> expires = new ArrayList<Pair<byte[], Integer>>(tupleList.size() * 2);
			int i = 0;
			if (CollectionUtils.isNotEmpty(tupleList)) {
				for (Tuple<String, byte[], Integer> tupe : tupleList) {
					keys[i] = tupe.getKey().getBytes("UTF-8");
					keyvalues[2 * i] = tupe.getKey().getBytes("UTF-8");
					keyvalues[2 * i + 1] = tupe.getKey().getBytes("UTF-8");
					expires.add(new Pair<byte[], Integer>(keys[i], tupe.getAttachment()));
					i++;
				}
			}
			String watchResponse = jedis.watch(keys);
			if (OK.equalsIgnoreCase(watchResponse)) {
				Transaction transaction = jedis.multi();
				transaction.msetnx(keyvalues);
				for (Pair<byte[], Integer> expire : expires) {
					if (expire.getValue() != null && expire.getValue() >= 0) {
						transaction.expire(expire.getKey(), expire.getValue());
					}
				}
				List<Object> responses = transaction.exec();
				boolean brk = false;
				if (CollectionUtils.isNotEmpty(responses)) {
					for (Object response : responses) {
						if (null != response) {
							if (OK.equalsIgnoreCase(response.toString())) {
								continue;
							} else if (SUCCESS.equals((Long) response)) {
								continue;
							} else {
								brk = true;
								break;
							}
						}
					}
					if (!brk) {
						result = true;
					}
				}
			}
		} catch (Exception e) {
			jedis.unwatch();
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

	public boolean setIfPresent(String key, byte[] value, Integer expireInSecs) {
		return setIfPresent(null, key, value, expireInSecs, true);
	}

	public boolean setIfPresent(Integer db, String key, byte[] value, Integer expireInSecs, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Pipeline pipeline = jedis.pipelined();
			Response<String> setResponse = pipeline.set(key.getBytes("UTF-8"), value, KEY_EXISTS);
			Response<Long> expireResponse = null;
			if (expireInSecs != null && expireInSecs > 0) {
				expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInSecs);
			}
			pipeline.sync();
			if (OK.equalsIgnoreCase(setResponse.get())) {
				if (null != expireResponse) {
					result = SUCCESS.equals(expireResponse.get());
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

	public boolean set(String key, byte[] bytes, Integer expireInSecs) {
		return set(null, key, bytes, expireInSecs, true);
	}
	
	public long getTTL(final String key) {
		Jedis jedis = null;
		long ttl = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			ttl = jedis.ttl(key);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			closeResource(jedis);
		}
		return ttl;
	}

	public boolean set(Integer db, String key, byte[] bytes, Integer expireInSecs, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			String response = null;
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (expireInSecs != null && expireInSecs > 0) {
				response = jedis.setex(key.getBytes("UTF-8"), expireInSecs, bytes);
			} else {
				response = jedis.set(key.getBytes("UTF-8"), bytes);
			}
			if (OK.equalsIgnoreCase(response)) {
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

	public boolean multiSet(List<Tuple<String, byte[], Integer>> kvs) {
		return multiSet(null, kvs, true);
	}

	public boolean multiSet(Integer db, List<Tuple<String, byte[], Integer>> kvs, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (CollectionUtils.isNotEmpty(kvs)) {
				byte[][] keysvalues = new byte[kvs.size() * 2][];
				int i = 0;
				for (Tuple<String, byte[], Integer> kv : kvs) {
					keysvalues[2 * i] = kv.getKey().getBytes("UTF-8");
					keysvalues[2 * i + 1] = kv.getValue();
					i++;
				}

				Pipeline pipeline = jedis.pipelined();
				Response<String> response = pipeline.mset(keysvalues);
				List<Response<Long>> responseList = new ArrayList<Response<Long>>();
				for (Tuple<String, byte[], Integer> kv : kvs) {
					if (null != kv.getAttachment() && kv.getAttachment() >= 0) {
						responseList.add(pipeline.expire(kv.getKey().getBytes("UTF-8"), kv.getAttachment()));
					}
				}

				pipeline.sync();

				boolean brk = false;

				if (OK.equalsIgnoreCase(response.get())) {
					for (Response<Long> resp : responseList) {
						if (null == resp || resp.get() <= 0) {
							brk = true;
							break;
						}
					}
					if (!brk) {
						result = true;
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

	public byte[] get(String key) {
		return get(null, key, true);
	}

	public byte[] get(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		byte[] result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.get(key.getBytes("UTF-8"));
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

	public Map<String, byte[]> multiGet(String... keys) {
		return multiGet(null, true, keys);
	}

	public Map<String, byte[]> multiGet(Integer db, boolean isSkipSelect, String... keys) {
		Jedis jedis = null;
		Map<String, byte[]> result = new HashMap<String, byte[]>();
		try {
			if (ArrayUtils.isNotEmpty(keys)) {
				byte[][] ks = new byte[keys.length][];
				for (int i = 0; i < keys.length; i++) {
					ks[i] = keys[i].getBytes("UTF-8");
				}
				jedis = JEDIS_POOL.getResource();
				if (!isSkipSelect) {
					selectIndex(jedis, db);
				}
				List<byte[]> response = jedis.mget(ks);
				if (CollectionUtils.isNotEmpty(response)) {
					for (int i = 0; i < response.size(); i++) {
						result.put(keys[i], response.get(i));
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

	public boolean del(String key) {
		return del(null, key, true);
	}

	public boolean del(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long response = jedis.del(key.getBytes("UTF-8"));
			if (response == 1) {
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

	public boolean del(String... keys) {
		return del(null, true, keys);
	}

	public boolean del(Integer db, boolean isSkipSelect, String... keys) {
		Jedis jedis = null;
		boolean result = false;
		try {
			if (ArrayUtils.isNotEmpty(keys)) {

				byte[][] ks = new byte[keys.length][];
				for (int i = 0; i < keys.length; i++) {
					if (null != keys[i]) {
						ks[i] = keys[i].getBytes("UTF-8");
					}
				}

				jedis = JEDIS_POOL.getResource();
				if (!isSkipSelect) {
					selectIndex(jedis, db);
				}
				Long response = jedis.del(ks);
				if (response > 0) {
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

	public Long incr(String key, long deta, Integer expireInsecs) {
		return incr(null, key, deta, expireInsecs, true);
	}

	/**
	 *
	 * 执行原子增加一个浮点数，expireInSecs单位是秒，如果为null或者小于等于零的话就不设置过期时间
	 */
	public Long incr(Integer db, String key, long deta, Integer expireInsecs, boolean isSkipSelect) {
		Jedis jedis = null;
		Long result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (null == expireInsecs) {
				if (deta > 0) {
					if (deta == 1) {
						result = jedis.incr(key.getBytes("UTF-8"));
					} else {
						result = jedis.incrBy(key.getBytes("UTF-8"), deta);
					}
				} else {
					if (deta == -1) {
						result = jedis.decr(key.getBytes("UTF-8"));
					} else if (deta < 0) {
						result = jedis.decrBy(key.getBytes("UTF-8"), deta);
					}
				}
			} else {
				Pipeline pipeline = jedis.pipelined();
				Response<Long> incrResponse = null;
				if (deta > 0) {
					if (deta == 1) {
						incrResponse = pipeline.incr(key.getBytes("UTF-8"));
					} else {
						incrResponse = pipeline.incrBy(key.getBytes("UTF-8"), deta);
					}
				} else {
					if (deta == -1) {
						incrResponse = pipeline.decr(key.getBytes("UTF-8"));
					} else if (deta < 0) {
						incrResponse = pipeline.decrBy(key.getBytes("UTF-8"), deta);
					}
				}
				Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInsecs);
				if (null != incrResponse && null != expireResponse && expireResponse.get() >= 0) {
					result = incrResponse.get();
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

	public Double incrByFloat(String key, double deta, Integer expireInsecs) {
		return incrByFloat(null, key, deta, expireInsecs, true);
	}

	public Double incrByFloat(Integer db, String key, double deta, Integer expireInsecs, boolean isSkipSelect) {
		Jedis jedis = null;
		Double result = null;
		try {
			if (deta != 0.0) {
				jedis = JEDIS_POOL.getResource();
				if (!isSkipSelect) {
					selectIndex(jedis, db);
				}
				if (null == expireInsecs) {
					result = jedis.incrByFloat(key.getBytes("UTF-8"), deta);
				} else {
					Pipeline pipeline = jedis.pipelined();
					Response<Double> incrResponse = pipeline.incrByFloat(key.getBytes("UTF-8"), deta);
					Response<Long> expireResponse = pipeline.expire(key.getBytes("UTF-8"), expireInsecs);
					if (null != incrResponse && null != expireResponse && expireResponse.get() >= 0) {
						result = incrResponse.get();
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

	public boolean setIfAbsent(String key, String value) {
		return setIfAbsent(null, true, key, value);
	}

	public boolean setIfAbsent(Integer db, boolean isSkipSelect, String key, String value) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long setnx = jedis.setnx(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(value));
			if (setnx == 1) {
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

	/**
	 * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。 当 key 存在但不是字符串类型时，返回一个错误。
	 */
	public String getSet(String key, String value) {
		return getSet(null, true, key, value);
	}

	public String getSet(Integer db, boolean isSkipSelect, String key, String value) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[] set = jedis.getSet(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(value));
			result = Bytes.bytesToUtf8String(set);
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
