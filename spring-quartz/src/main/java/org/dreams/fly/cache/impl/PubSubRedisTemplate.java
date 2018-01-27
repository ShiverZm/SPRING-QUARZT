package org.dreams.fly.cache.impl;

import org.dreams.fly.common.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PubSubRedisTemplate extends KeyRedisTemplate {

	protected final static Logger LOG = LoggerFactory.getLogger(PubSubRedisTemplate.class);

	public void patternSubscribe(JedisPubSub jedisPubSub, String... patterns) {
		patternSubscribe(null, true, jedisPubSub, patterns);
	}

	public void patternSubscribe(Integer db, boolean isSkipSelect, JedisPubSub jedisPubSub, String... patterns) {
		Jedis jedis = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			jedis.psubscribe(jedisPubSub, patterns);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
	}

	public long publish(String channel, String message) {
		return publish(null, true, channel, message);
	}

	public long publish(Integer db, boolean isSkipSelect, String channel, String message) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.publish(Bytes.stringToUtf8Bytes(channel), Bytes.stringToUtf8Bytes(message));
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

	public void subscribe(JedisPubSub jedisPubSub, String... pattern) {
		subscribe(null, true, jedisPubSub, pattern);
	}

	public void subscribe(Integer db, boolean isSkipSelect, JedisPubSub jedisPubSub, String... pattern) {
		Jedis jedis = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			jedis.subscribe(jedisPubSub, pattern);
		} catch (Exception e) {
			handleJedisException(e);
			LOG.error(e.toString());
		} finally {
			if (!isSkipSelect) {
				selectIndex(jedis, DEFAULT_DATABASE);
			}
			closeResource(jedis);
		}
	}

	public List<String> pubsubChannels(String pattern) {
		return pubsubChannels(null, true, pattern);
	}

	public List<String> pubsubChannels(Integer db, boolean isSkipSelect, String pattern) {
		Jedis jedis = null;
		List<String> result = new ArrayList<String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}

			result = jedis.pubsubChannels(pattern);
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

	public Map<String, String> pubsubNumsub(String... patterns) {
		return pubsubNumsub(null, true, patterns);
	}

	public Map<String, String> pubsubNumsub(Integer db, boolean isSkipSelect, String... patterns) {
		Jedis jedis = null;
		Map<String, String> result = new HashMap<String, String>();
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.pubsubNumSub(patterns);
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

	public long pubsubNumPat() {
		return pubsubNumPat(null, true);
	}

	public long pubsubNumPat(Integer db, boolean isSkipSelect) {
		Jedis jedis = null;
		long result = 0l;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			result = jedis.pubsubNumPat();
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

	public void unsubscribe(JedisPubSub jedisPubSub, String... patterns) {
		unsubscribe(null, true, jedisPubSub, patterns);
	}

	public void unsubscribe(Integer db, boolean isSkipSelect, JedisPubSub jedisPubSub, String... patterns) {
		Jedis jedis = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (patterns == null) {
				jedisPubSub.unsubscribe();
			} else {
				jedisPubSub.unsubscribe(patterns);
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

	}

	public void patternUnsubscribe(JedisPubSub jedisPubSub, String... patterns) {
		patternUnsubscribe(null, true, jedisPubSub, patterns);
	}

	public void patternUnsubscribe(Integer db, boolean isSkipSelect, JedisPubSub jedisPubSub, String... patterns) {
		Jedis jedis = null;
		try {
			jedis = JEDIS_POOL.getResource();
			if (!isSkipSelect) {
				selectIndex(jedis, db);
			}
			if (patterns == null) {
				jedisPubSub.punsubscribe();
			} else {
				jedisPubSub.punsubscribe(patterns);
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
	}

}
