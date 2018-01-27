package org.dreams.fly.cache.impl;

import org.dreams.fly.common.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class KeyRedisTemplate extends AbsRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(KeyRedisTemplate.class);

	public boolean rmKeys(String... keys) {
		return rmKeys(null, true, keys);
	}

	public boolean rmKeys(Integer db, boolean isSkipSelect, String... keys) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			Long del = jedis.del(Bytes.stringsToBytes(keys));
			if (del >= 0) {
				result = true;
			}
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
			result = false;
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
		return result;
	}

	public byte[] serializationKey(String key) {
		return serializationKey(null, true, key);
	}

	public byte[] serializationKey(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		byte[] result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.dump(Bytes.stringToUtf8Bytes(key));
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

	public String deserializationKey(String key, Integer timeComplexity, byte[] byteKey) {
		return deserializationKey(null, true, key, timeComplexity, byteKey);
	}

	public String deserializationKey(Integer db, boolean isSkipSelect, String key, Integer timeComplexity,
			byte[] byteKey) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.restore(Bytes.stringToUtf8Bytes(key), timeComplexity, byteKey);
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

	public boolean exists(String key) {
		return exists(null, true, key);
	}

	public boolean exists(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.exists(Bytes.stringToUtf8Bytes(key));
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

	public boolean expireBySeconds(String key, Integer survivalSeconds) {
		return expireBySeconds(null, true, key, survivalSeconds);
	}

	public boolean expireBySeconds(Integer db, boolean isSkipSelect, String key, Integer survivalSeconds) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long expire = jedis.expire(Bytes.stringToUtf8Bytes(key), survivalSeconds);
			if (expire == 1) {
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

	public boolean expireByUnixTimestamp(String key, Integer unixTimestamp) {
		return expireByUnixTimestamp(null, true, key, unixTimestamp);
	}

	public boolean expireByUnixTimestamp(Integer db, boolean isSkipSelect, String key, Integer unixTimestamp) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long expire = jedis.expireAt(Bytes.stringToUtf8Bytes(key), unixTimestamp);
			if (expire == 1) {
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

	public List<String> keys(String pattern) {
		return keys(null, true, pattern);
	}

	public List<String> keys(Integer db, boolean isSkipSelect, String pattern) {
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[] rawKey = Bytes.stringToUtf8Bytes(pattern+"*");
			Set<byte[]> keys = jedis.keys(rawKey);
			if (keys.size() == 0) {
				return result;
			}
			for (byte[] key : keys) {
				result.add(Bytes.bytesToUtf8String(key));
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

	public boolean moveDB(String key, Integer dB) {
		return moveDB(null, true, key, dB);
	}

	public boolean moveDB(Integer db, boolean isSkipSelect, String key, Integer dB) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			Long move = jedis.move(Bytes.stringToUtf8Bytes(key), dB);
			if (move == 1) {
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

	public boolean persist(String key) {
		return persist(null, true, key);
	}

	public boolean persist(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			Long persist = jedis.persist(Bytes.stringToUtf8Bytes(key));
			if (persist == 1) {
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

	public boolean pexpire(String key, Integer survivalMS) {
		return pexpire(null, true, key, survivalMS);
	}

	public boolean pexpire(Integer db, boolean isSkipSelect, String key, Integer survivalMS) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long persist = jedis.pexpire(Bytes.stringToUtf8Bytes(key), (long) survivalMS);
			if (persist == 1) {
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

	public long pttl(String key) {
		return pttl(null, true, null);
	}

	public long pttl(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.pttl(Bytes.stringToUtf8Bytes(key));
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

	public String randomKey() {
		return randomKey(null, true);
	}

	public String randomKey(Integer db, boolean isSkipSelect) {
		Jedis jedis = null;
		String result = null;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.randomKey();
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

	public boolean rename(String oldKey, String newKey) {
		return rename(null, true, oldKey, newKey);
	}

	public boolean rename(Integer db, boolean isSkipSelect, String oldKey, String newKey) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			String rename = jedis.rename(Bytes.stringToUtf8Bytes(oldKey), Bytes.stringToUtf8Bytes(newKey));
			if (rename != null && "ok".equals(rename)) {
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

	public boolean renameIfAbsent(String oldKey, String newKey) {
		return renameIfAbsent(null, true, oldKey, newKey);
	}

	public boolean renameIfAbsent(Integer db, boolean isSkipSelect, String oldKey, String newKey) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long renamenx = jedis.renamenx(Bytes.stringToUtf8Bytes(oldKey), Bytes.stringToUtf8Bytes(newKey));
			if (renamenx == 1) {
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
	 * 以秒为单位返回 key 的剩余生存时间
	 * 
	 * @param key
	 * @return
	 */
	public long ttl(String key) {
		return ttl(null, true, key);
	}

	public long ttl(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.ttl(Bytes.stringToUtf8Bytes(key));
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

	public String getKeyType(String key) {
		return getKeyType(null, true, key);
	}

	public String getKeyType(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		String result = null;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.type(Bytes.stringToUtf8Bytes(key));
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