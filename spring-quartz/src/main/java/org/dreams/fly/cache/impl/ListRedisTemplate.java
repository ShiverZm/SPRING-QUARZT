package org.dreams.fly.cache.impl;

import org.dreams.fly.common.bytes.Bytes;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ListRedisTemplate extends KeyRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(ListRedisTemplate.class);

	
	public List<String> leftPop(String key, Long start, Long end) {
		return leftPop(null, true, key, start, end);
	}
	public List<String> leftPop(Integer db, boolean isSkipSelect, String key, Long start, Long end) {
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Pipeline pipeline = jedis.pipelined();
			Response<List<byte[]>> lrange = pipeline.lrange(key.getBytes("UTF-8"), start, end);
			pipeline.ltrim(key.getBytes("UTF-8"), end + 1, -1L);
			pipeline.sync();
			if(CollectionUtils.isNotEmpty(lrange.get())){
				result = Bytes.listBytesToListString(lrange.get());
			}
		} catch (Exception e) {
			LOG.error("selectAllClear key:{} error:{}", key, e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}
	
	public List<String> rmAll(String key) {
		return rmAll(null, true, key);
	}
	
	public List<String> rmAll(Integer db, boolean isSkipSelect, String key){
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Pipeline pipeline = jedis.pipelined();
			Response<List<byte[]>> lrange = pipeline.lrange(key.getBytes("UTF-8"), 0, -1);
			pipeline.del(key.getBytes("UTF-8"));
			pipeline.sync();
			if(CollectionUtils.isNotEmpty(lrange.get())){
				result = Bytes.listBytesToListString(lrange.get());
			}
		} catch (Exception e) {
			LOG.error("selectAllClear key:{} error:{}", key, e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}
	
	public List<String> blockLeftPop(long timeout, TimeUnit timeUnit, String... keys) {
		return blockLeftPop(null, true, timeout, timeUnit, keys);
	}

	public List<String> blockLeftPop(Integer db, boolean isSkipSelect, long timeout, TimeUnit timeUnit,
			String... keys) {
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			int timeoutInSecs = (int) TimeUnit.SECONDS.convert(timeout, timeUnit);
			byte[][] stringsToBytes = Bytes.stringsToBytes(keys);
			List<byte[]> list = jedis.blpop(timeoutInSecs, stringsToBytes);
			if(null != list){
				result = Bytes.byteListToUtf8StringList(list);
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public List<String> blockRightPop(long timeout, TimeUnit timeUnit, String... keys) {
		return blockRightPop(null, true, timeout, timeUnit, keys);
	}

	public List<String> blockRightPop(Integer db, boolean isSkipSelect, long timeout, TimeUnit timeUnit,
			String... keys) {
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			int timeoutInSecs = (int) TimeUnit.SECONDS.convert(timeout, timeUnit);
			byte[][] stringsToBytes = Bytes.stringsToBytes(keys);
			List<byte[]> list = jedis.brpop(timeoutInSecs, stringsToBytes);
			if(null != list){
				result = Bytes.byteListToUtf8StringList(list);
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public String blockRightPopLeftPush(String source, String destination, Integer timeout) {
		return blockRightPopLeftPush(null, true, source, destination, timeout);
	}

	public String blockRightPopLeftPush(Integer db, boolean isSkipSelect, String source, String destination,
			Integer timeout) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[] brpoplpush = jedis.brpoplpush(Bytes.stringToUtf8Bytes(source), Bytes.stringToUtf8Bytes(destination),
					timeout);
			result = Bytes.bytesToUtf8String(brpoplpush);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occcur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public String getByIndex(String key, Integer index) {
		return getByIndex(null, true, key, index);
	}

	public String getByIndex(Integer db, boolean isSkipSelect, String key, Integer index) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[] lindex = jedis.lindex(Bytes.stringToUtf8Bytes(key), (long) index);
			result = Bytes.bytesToUtf8String(lindex);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public long insert(String key, String pivot, String value, boolean beforeAndAfter) {
		return insert(null, true, key, pivot, value, beforeAndAfter);
	}

	public long insert(Integer db, boolean isSkipSelect, String key, String pivot, String value,
			boolean beforeAndAfter) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (beforeAndAfter) {
				result = jedis.linsert(key, LIST_POSITION.BEFORE, pivot, value);
			} else {
				result = jedis.linsert(key, LIST_POSITION.AFTER, pivot, value);
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public long size(String key) {
		return size(null, key, true);
	}

	public long size(Integer db, String key, boolean isSkipSelect) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.llen(Bytes.stringToUtf8Bytes(key));
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public void rightPush(String key, String value) {
		rightPush(null, true, key, value);
	}

	public void rightPush(Integer db, boolean isSkipSelect, String key, String value) {
		Jedis jedis = null;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[] rawKey = Bytes.stringToUtf8Bytes(key);
			byte[] rawValue = Bytes.stringToUtf8Bytes(value);
			jedis.rpush(rawKey, rawValue);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

	}

	public String leftPop(String key) {
		return leftPop(null, true, key);
	}

	public String leftPop(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		String result = null;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[] rawKey = Bytes.stringToUtf8Bytes(key);
			result = Bytes.bytesToUtf8String(jedis.lpop(rawKey));
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public Long leftPush(String key, String... values) {
		return leftPush(null, true, key, values);
	}

	public Long leftPush(Integer db, boolean isSkipSelect, String key, String... values) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[] rawKey = Bytes.stringToUtf8Bytes(key);
			byte[][] stringsToBytes = Bytes.stringsToBytes(values);
			result = jedis.lpush(rawKey, stringsToBytes);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public Long leftPushX(String key, String value) {
		return leftPushX(null, true, key, value);
	}

	public Long leftPushX(Integer db, boolean isSkipSelect, String key, String value) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.lpushx(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(value));
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public List<byte[]> getBySubScript(String key, long start, long end) {
		return getBySubScript(null, true, key, start, end);
	}

	public List<byte[]> getBySubScript(Integer db, boolean isSkipSelect, String key, long start, long end) {
		Jedis jedis = null;
		List<byte[]> result = new ArrayList<byte[]>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[] rawKey = Bytes.stringToUtf8Bytes(key);
			List<byte[]> lrangeList = jedis.lrange(rawKey, start, end);
			result = lrangeList;
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public long leftOrRightRemove(String key, Integer count, String value) {
		return leftOrRightRemove(null, true, key, count, value);
	}

	public long leftOrRightRemove(Integer db, boolean isSkipSelect, String key, Integer count, String value) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.lrem(Bytes.stringToUtf8Bytes(key), count, Bytes.stringToUtf8Bytes(value));
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public boolean assignmentByKey(String key, Integer index, String value) {
		return assignmentByKey(null, true, key, index, value);
	}

	public boolean assignmentByKey(Integer db, boolean isSkipSelect, String key, Integer index, String value) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			String lset = jedis.lset(Bytes.stringToUtf8Bytes(key), index, Bytes.stringToUtf8Bytes(value));
			if (lset != null && "ok".equals(lset)) {
				result = true;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public boolean shearByIndex(String key, Integer start, Integer stop) {
		return shearByIndex(null, true, key, start, stop);
	}

	public boolean shearByIndex(Integer db, boolean isSkipSelect, String key, Integer start, Integer stop) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			String ltrim = jedis.ltrim(Bytes.stringToUtf8Bytes(key), start, stop);
			if (ltrim != null && "ok".equals(ltrim)) {
				result = true;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public String rightPop(String key) {
		return rightPop(null, true, key);
	}

	public String rightPop(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[] rpop = jedis.rpop(Bytes.stringToUtf8Bytes(key));
			result = Bytes.bytesToUtf8String(rpop);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	public String rightPopLeftPush(String source, String destination) {
		return rightPopLeftPush(null, true, source, destination);
	}

	public String rightPopLeftPush(Integer db, boolean isSkipSelect, String source, String destination) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[] rpop = jedis.rpoplpush(Bytes.stringToUtf8Bytes(source), Bytes.stringToUtf8Bytes(destination));
			result = Bytes.bytesToUtf8String(rpop);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	/**
	 * 将一个或多个值 value 插入到列表 key 的表尾(最右边)
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	public long rightPush(String key, byte[][] values) {
		return rightPush(null, true, key, values);
	}

	public long rightPush(Integer db, boolean isSkipSelect, String key, byte[][] values) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.rpush(Bytes.stringToUtf8Bytes(key), values);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

	/***
	 * 将值 value 插入到列表 key 的表尾，当且仅当 key 存在并且是一个列表
	 * 
	 * @param key
	 * @param values
	 * @return
	 */
	public long rightPushX(String key, String... values) {
		return rightPushX(null, true, key, values);
	}

	public long rightPushX(Integer db, boolean isSkipSelect, String key, String... values) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[][] stringsToBytes = Bytes.stringsToBytes(values);
			result = jedis.rpushx(Bytes.stringToUtf8Bytes(key), stringsToBytes);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error("occur error:", e);
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}

		return result;
	}

}
