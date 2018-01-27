package org.dreams.fly.cache.impl;


import org.dreams.fly.common.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SetRedisTemplate extends KeyRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(SetRedisTemplate.class);

	public long add(String key, String... member) {
		return add(null, true, key, member);
	}

	public long add(Integer db, boolean isSkipSelect, String key, String... member) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			byte[][] stringsToBytes = Bytes.stringsToBytes(member);
			result = jedis.sadd(Bytes.stringToUtf8Bytes(key), stringsToBytes);
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

	public long size(String key) {
		return size(null, true, key);
	}

	public long size(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.scard(Bytes.stringToUtf8Bytes(key));
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

	public Set<String> diff(String... keys) {
		return diff(null, true, keys);
	}

	public Set<String> diff(Integer db, boolean isSkipSelect, String... keys) {
		Jedis jedis = null;
		Set<String> result = new HashSet<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> sdiff = jedis.sdiff(Bytes.stringsToBytes(keys));
			result = Bytes.setBytestoSetString(sdiff);
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

	public long diffStore(String destination, String... keys) {
		return diffStore(null, true, destination, keys);
	}

	public long diffStore(Integer db, boolean isSkipSelect, String destination, String... keys) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] stringsToBytes = Bytes.stringsToBytes(keys);
			result = jedis.sdiffstore(Bytes.stringToUtf8Bytes(destination), stringsToBytes);
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

	public Set<String> inter(String... keys) {
		return inter(null, true, keys);
	}

	public Set<String> inter(Integer db, boolean isSkipSelect, String... keys) {
		Jedis jedis = null;
		Set<String> result = new HashSet<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> sdiff = jedis.sinter(Bytes.stringsToBytes(keys));
			result = Bytes.setBytestoSetString(sdiff);
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

	public long interStore(String destination, String... keys) {
		return interStore(null, true, destination, keys);
	}

	public long interStore(Integer db, boolean isSkipSelect, String destination, String... keys) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] stringsToBytes = Bytes.stringsToBytes(keys);
			result = jedis.sinterstore(Bytes.stringToUtf8Bytes(destination), stringsToBytes);
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

	public boolean isMember(String key, String member) {
		return isMember(null, true, key, member);
	}

	public boolean isMember(Integer db, boolean isSkipSelect, String key, String member) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.sismember(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(member));
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

	public Set<String> members(String key) {
		return members(null, true, key);
	}

	public Set<String> members(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		Set<String> result = new HashSet<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> smembers = jedis.smembers(Bytes.stringToUtf8Bytes(key));
			result = Bytes.setBytestoSetString(smembers);
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

	public boolean move(String source, String destination, String member) {
		return move(null, true, source, destination, member);
	}

	public boolean move(Integer db, boolean isSkipSelect, String source, String destination, String member) {
		Jedis jedis = null;
		boolean result = false;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Long smove = jedis.smove(Bytes.stringToUtf8Bytes(source), Bytes.stringToUtf8Bytes(destination),
					Bytes.stringToUtf8Bytes(member));
			if (smove == 1) {
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

	public String pop(String key) {
		return pop(null, true, key);
	}

	public String pop(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		String result = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[] spop = jedis.spop(Bytes.stringToUtf8Bytes(key));
			result = Bytes.bytesToUtf8String(spop);
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

	public List<String> randomMember(String key, Integer number) {
		return randomMember(null, true, key, number);
	}

	public List<String> randomMember(Integer db, boolean isSkipSelect, String key, Integer number) {
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			List<byte[]> srandmember = jedis.srandmember(Bytes.stringToUtf8Bytes(key), number);
			result = Bytes.listBytesToListString(srandmember);
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

	public long removeMember(String key, String... members) {
		return removeMember(null, true, key, members);
	}

	public long removeMember(Integer db, boolean isSkipSelect, String key, String... members) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] stringsToBytes = Bytes.stringsToBytes(members);
			result = jedis.srem(Bytes.stringToUtf8Bytes(key), stringsToBytes);
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

	public Set<String> union(String... keys) {
		return union(null, true, keys);
	}

	public Set<String> union(Integer db, boolean isSkipSelect, String... keys) {
		Jedis jedis = null;
		Set<String> result = new HashSet<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] stringsToBytes = Bytes.stringsToBytes(keys);
			Set<byte[]> sunion = jedis.sunion(stringsToBytes);
			result = Bytes.setBytestoSetString(sunion);
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

	public long unionStore(String destination, String... keys) {
		return unionStore(null, true, destination, keys);
	}

	public long unionStore(Integer db, boolean isSkipSelect, String destination, String... keys) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] stringsToBytes = Bytes.stringsToBytes(keys);
			result = jedis.sunionstore(Bytes.stringToUtf8Bytes(destination), stringsToBytes);
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
