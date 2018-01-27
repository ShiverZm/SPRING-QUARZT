package org.dreams.fly.cache.impl;

import org.dreams.fly.common.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
/**
 * @author 艾国梁
 * redis 操作命令
   zadd key score member            添加元素到集合，元素在集合中存在则更新对应score
   zrem key member                  删除指定元素，1表示成功，如果元素不存在返回0
   zincrby key incr member          按照incr幅度增加对应member的score值，返回score值
   zrank key member                 返回指定元素在集合中的排名(下标)，集合中元素是按score从小到大排序的
   zrevrank key member              同上，但是集合中元素是按score从大到小排序
   zrange key start end             类似lrange操作是从集合中去指定区间的元素。返回的是有序的结果
   zrevrange key start end          同上，返回结果是按score逆序的
   zcard key                        返回集合中元素个数
   zscore key element               返回给定元素对应的score
   zremrangebyrank key min max      删除集合中排名在给定区间的元素(权值从小到大排序)
 */
public class SortedSetRedisTemplate extends KeyRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(SortedSetRedisTemplate.class);

	/**
	 * 向集合添加元素
	 * @param key 
	 * @param score
	 * @param value
	 * @return
	 */
	public Boolean add(String key, double score, String value) {
		return add(null, true, key, score, value);
	}
	
	public Boolean add(Integer db, boolean isSkipSelect, String key, double score, String value) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			Long zadd = jedis.zadd(Bytes.stringToUtf8Bytes(key), score, Bytes.stringToUtf8Bytes(value));
			if (zadd >= 0) {
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
	 * 二进制map形式添加元素
	 * @param key
	 * @param map
	 * @return
	 */
	public Boolean add(String key, Map<byte[], Double> map) {
		return add(null, true, key, map);
	}

	public Boolean add(Integer db, boolean isSkipSelect, String key, Map<byte[], Double> map) {
		Jedis jedis = null;
		boolean result = false;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			jedis.zadd(Bytes.stringToUtf8Bytes(key), map);
			result = true;
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
	 * 获取集合元素的个数
	 * @param key
	 * @return
	 */
	public Long size(String key) {
		return size(null, true, key);
	}

	public Long size(Integer db, boolean isSkipSelect, String key) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.zcard(Bytes.stringToUtf8Bytes(key));
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

	public long count(String key, double min, double max) {
		return count(null, true, key, min, max);
	}

	public long count(Integer db, boolean isSkipSelect, String key, double min, double max) {
		Jedis jedis = null;
		long result = 0l;

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.zcount(Bytes.stringToUtf8Bytes(key), min, max);
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
	 * 把当前 member 的 score 加 increment
	 * @param key
	 * @param increment
	 * @param member
	 * @return
	 */
	public double incrByScore(String key, double increment, String member) {
		return incrByScore(null, true, key, increment, member);
	}

	public double incrByScore(Integer db, boolean isSkipSelect, String key, double increment,
			String member) {
		Jedis jedis = null;
		double result = 0.0;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.zincrby(Bytes.stringToUtf8Bytes(key), increment, Bytes.stringToUtf8Bytes(member));
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
    * 根据 score 从小到大排序
    * @param key
    * @param start
    * @param end
    * @return
    */
	public Set<String> range(String key, long start, long end) {
		return range(null, true, key, start, end);
	}
	
	
	public Set<String> range(Integer db, boolean isSkipSelect, String key, long start, long end) {
		Jedis jedis = null;
		Set<String> result = new LinkedHashSet<String>();

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> zrange = jedis.zrange(Bytes.stringToUtf8Bytes(key), start, end);
			result = Bytes.setBytestaSortSetString(zrange);
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
	 * 根据 score从小到大排序，获取当前member的排名
	 * @param key
	 * @param member
	 * @return
	 */
	public long rank(String key, String member) {
		return rank(null, true, key, member);
	}

	
	public long rank(Integer db, boolean isSkipSelect, String key, String member) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.zrank(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(member));
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
	 * 根据score区间值获取有序的集合
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public Set<String> rangeStore(String key, double min, double max) {
		return rangeStore(null, true, key, min, max);
	}

	public Set<String> rangeStore(Integer db, boolean isSkipSelect, String key, double min, double max) {
		Jedis jedis = null;
		Set<String> result = new LinkedHashSet<String>();

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> zrangeByScore = jedis.zrangeByScore(Bytes.stringToUtf8Bytes(key), min, max);
			result = Bytes.setBytestaSortSetString(zrangeByScore);
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

	
	public Set<String> rangeStore(String key, double min, double max, Integer offset,
			Integer count) {
		return rangeStore(null, true, key, min, max, offset, count);
	}

	public Set<String> rangeStore(Integer db, boolean isSkipSelect, String key, double min,
			double max, Integer offset, Integer count) {
		Jedis jedis = null;
		Set<String> result = new LinkedHashSet<String>();

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> zrangeByScore = jedis.zrangeByScore(Bytes.stringToUtf8Bytes(key), min, max, offset, count);
			result = Bytes.setBytestaSortSetString(zrangeByScore);
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

	
	
	
	public Set<String> zrevrangeStore(String key, double min, double max, Integer offset,
			Integer count) {
		return zrevrangeStore(null, true, key, min, max, offset, count);
	}

	public Set<String> zrevrangeStore(Integer db, boolean isSkipSelect, String key, double min,
			double max, Integer offset, Integer count) {
		Jedis jedis = null;
		Set<String> result = new LinkedHashSet<String>();

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			Set<byte[]> zrangeByScore = jedis.zrevrangeByScore(Bytes.stringToUtf8Bytes(key), max, min, offset, count);
			result = Bytes.setBytestaSortSetString(zrangeByScore);
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

	
	
	
	public Set<Tuple> rangeStoreWithScore(String key, double min, double max) {
		return rangeStoreWithScore(null, true, key, min, max);
	}

	public Set<Tuple> rangeStoreWithScore(Integer db, boolean isSkipSelect, String key, double min,
			double max) {
		Jedis jedis = null;
		Set<Tuple> result = new HashSet<Tuple>();

		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.zrangeByScoreWithScores(key, min, max);
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
	 * 根据 members 删除元素
	 * @param key
	 * @param members
	 * @return
	 */
	public long remove(String key, String... members) {
		return remove(null, true, key, members);
	}

	public long remove(Integer db, boolean isSkipSelect, String key, String... members) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			byte[][] stringsToBytes = Bytes.stringsToBytes(members);
			result = jedis.zrem(Bytes.stringToUtf8Bytes(key), stringsToBytes);
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
	 * 删除排名好的区间元素总比如1 - 10名 ，删除 2-4 名
	 * @param key
	 * @param start
	 * @param stop
	 * @return
	 */
	public long removeRangeByRank(String key, Integer start, Integer stop) {
		return removeRangeByRank(null, true, key, start, stop);
	}

	public long removeRangeByRank(Integer db, boolean isSkipSelect, String key, Integer start, Integer stop) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.zremrangeByRank(Bytes.stringToUtf8Bytes(key), start, stop);
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
	 * 同remove 增加了 score 范围
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public long removeRangeByScore(String key, double min, double max) {
		return removeRangeByScore(null, true, key, min, max);
	}

	public long removeRangeByScore(Integer db, boolean isSkipSelect, String key, double min, double max) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.zremrangeByScore(Bytes.stringToUtf8Bytes(key), min, max);
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
	 * 根据score大到小排序
	 * @param key
	 * @param start
	 * @param stop
	 * @return
	 */
	public Set<String> reverseRange(String key, long start, long stop) {
		return reverseRange(null, true, key, start, stop);
	}

	public Set<String> reverseRange(Integer db, boolean isSkipSelect, String key, long start, long stop) {
		Jedis jedis = null;
		Set<String> result = new LinkedHashSet<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			Set<byte[]> zrevrange = jedis.zrevrange(Bytes.stringToUtf8Bytes(key), start, stop);
			result = Bytes.setBytestaSortSetString(zrevrange);
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
	 * 根据 score 大到小排序，获取当前member的下标
	 * @param key
	 * @param member
	 * @return
	 */
	public long reverseRank(String key, String member) {
		return reverseRank(null, true, key, member);
	}

	public long reverseRank(Integer db, boolean isSkipSelect, String key, String member) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.zrevrank(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(member));
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
	 * 同 reverseRank 增加了score范围
	 * @param key
	 * @param min
	 * @param max
	 * @return
	 */
	public Set<String> reverseRangeByScore(String key, double min, double max) {
		return reverseRangeByScore(null, true, key, min, max);
	}

	public Set<String> reverseRangeByScore(Integer db, boolean isSkipSelect, String key, double min, double max) {
		Jedis jedis = null;
		Set<String> result = new LinkedHashSet<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			Set<byte[]> zrevrangeByScore = jedis.zrevrangeByScore(Bytes.stringToUtf8Bytes(key), max, min);
			result = Bytes.setBytestaSortSetString(zrevrangeByScore);
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
	 * 根据 member 获取 member 的 score
	 * @param key
	 * @param member
	 * @return
	 */
	public double getScoreByKeyAndMember(String key, String member) {
		return getScoreByKeyAndMember(null, true, key, member);
	}

	public double getScoreByKeyAndMember(Integer db, boolean isSkipSelect, String key, String member) {
		Jedis jedis = null;
		double result = 0.0;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.zscore(Bytes.stringToUtf8Bytes(key), Bytes.stringToUtf8Bytes(member));
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
			result = jedis.zunionstore(Bytes.stringToUtf8Bytes(destination), stringsToBytes);
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
			result = jedis.zinterstore(Bytes.stringToUtf8Bytes(destination), stringsToBytes);
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
