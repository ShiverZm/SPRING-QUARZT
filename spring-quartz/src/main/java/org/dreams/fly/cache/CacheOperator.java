package org.dreams.fly.cache;

import org.dreams.fly.common.JsonUtils;
import org.dreams.fly.common.bytes.Bytes;
import  org.dreams.fly.cache.impl.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CacheOperator {

	protected CacheOperator() {

	}

	// 获取 String 的 模板实例
	public static OptString getOptString() {
		return OptString.getInstance();
	}

	// 获取 hash 的 模板实例
	public static OptHash getOptHash() {
		return OptHash.getInstance();
	}

	// 获取 list 的 模板实例
	public static OptList getOptList() {
		return OptList.getInstance();
	}

	// 获取 set 的 模板实例
	public static OptSet getOptSet() {
		return OptSet.getInstance();
	}

	// 获取 zset 的 模板实例
	public static OptZSet getOptZSet() {
		return OptZSet.getInstance();
	}

	// 获取 hash 的 模板实例
	public static OptPubSub getOptPubSub() {
		return OptPubSub.getInstance();
	}

	/**
	 * 操作 REDIS STRING 工具类
	 */
	public static class OptString extends StringRedisTemplate {

		private static final class InstanceHolder {
			private static final OptString INSTANCE = new OptString();
		}

		protected static OptString getInstance() {
			return InstanceHolder.INSTANCE;
		}

		private OptString() {

		}

		public Boolean setValueToByteArray(final String namespace, final String key, final Object value,
				final int expire, final TimeUnit timeUnit) {
			Integer expireInSecs = getTimes(expire, timeUnit);
			return setValueToByteArray(namespace, key, value, expireInSecs);
		}

		public Boolean setValueToByteArray(final String namespace, final String key, final Object value,
				final Integer expireInSecs) {
			if (null == value) {
				return false;
			}
			String redisKey = getFullKey(namespace, key);
			return set(redisKey, Bytes.objectToBytes(value), expireInSecs);
		}

		/**
		 * 获得对象
		 */
		@SuppressWarnings("unchecked")
        public <T> T getValueFromByteArray(final String namespace, final String key) {
			if (null != key) {
				String fullKey = getFullKey(namespace, key);
				byte[] value = get(fullKey);
				if(null != value){
					return (T) Bytes.bytesToObject(value);
				}
			}
			return null;
		}

		/**
		 * 以JSON串的形式将对象存储到REDIS中
		 */
		public Boolean setValueToJsonString(final String namespace, final String key, final Object value,
				final int expire, final TimeUnit timeUnit) {
			Integer expireInSecs = getTimes(expire, timeUnit);
			return setValueToJsonString(namespace, key, value, expireInSecs);
		}

		public Boolean setValueToJsonString(final String namespace, final String key, final Object value,
				final Integer expireInSecs) {
			if (null == value) {
				return false;
			}
			String redisKey = getFullKey(namespace, key);
			return set(redisKey, Bytes.stringToUtf8Bytes(JsonUtils.toJson(value)), expireInSecs);
		}

		/**
		 * 获得对象
		 */
		public <T> T getValueFromJsonString(final String namespace, final String key, final Class<T> clazz) {
			if (null != key && null != clazz) {
				String fullKey = getFullKey(namespace, key);
				String value = Bytes.bytesToUtf8String(get(fullKey));
				if (StringUtils.isNotBlank(value)) {
					return JsonUtils.fromJson(value, clazz);
				}
			}
			return null;
		}

		/**
		 * 将KEY永久的添加到缓存中
		 */
		public boolean set(final String namespace, final String key, final String value) {
			String redisKey = getFullKey(namespace, key);
			return set(redisKey, Bytes.stringToUtf8Bytes(value), null);
		}

		/**
		 * 将KEY添加到缓存中并设置过期时间
		 */
		public boolean set(final String namespace, final String key, final String value, final int expire,
				final TimeUnit timeUnit) {
			Integer expireInSecs = getTimes(expire, timeUnit);
			return set(namespace, key, value, expireInSecs);
		}

		public boolean set(final String namespace, final String key, final String value, final Integer expireInSecs) {
			String fullKey = getFullKey(namespace, key);
			return set(fullKey, Bytes.stringToUtf8Bytes(value), expireInSecs);
		}

		/**
		 * 获取KEY对应的缓存的是VALUE
		 */
		public String get(final String namespace, final String key) {
			String fullKey = getFullKey(namespace, key);
			return Bytes.bytesToUtf8String(get(fullKey));
		}

		/**
		 *
		 * @param namespace
		 * @param key
		 * @return 获取剩余有效时间
		 */
		public long getTTL(final String namespace, final String key) {
			String fullKey = getFullKey(namespace, key);
			return getTTL(fullKey);
		}

	}

	/**
	 * REDIS HASH 操作工具类
	 */
	public static class OptHash extends HashRedisTemplate {

		private static final class InstanceHolder {
			private static final OptHash INSTANCE = new OptHash();
		}

		protected static OptHash getInstance() {
			return InstanceHolder.INSTANCE;
		}

		private OptHash() {

		}

		/**
		 * 将整个MAP以HASH的形式进行存储
		 */
		public boolean multiSetMap(final String namespace, final String key, final Map<String, ? extends Object> map,
				final Integer expireInSecs) {
			if (map == null) {
				return false;
			}
			String redisKey = getFullKey(namespace, key);
			Map<byte[], byte[]> mapByte = Bytes.stringObjectMapToByteMap(map);
			return muiltSet(redisKey, mapByte, expireInSecs);
		}

		/**
		 * 获取KEY对应的HASH的值
		 */
		public Map<String, ? extends Object> getMap(final String namespace, final String key) {
			String fullKey = getFullKey(namespace, key);
			Map<byte[], byte[]> value = getAll(fullKey);
			return Bytes.byteMapToStringObjectMap(value);
		}

		/**
		 * 获取KEY,field 对应的 Serializable 的值
		 * @param key
		 * @param fieldName
		 * @return
		 */
		public Object getMapByFields(final String key, final String fieldName) {
			byte[] hashGet = get(key, Bytes.stringToUtf8Bytes(fieldName));
			if(ArrayUtils.isEmpty(hashGet)){
				return null;
			}
			Object obj = Bytes.bytesToObject(hashGet);
			if(obj == null){
				return null;
			}
			return obj;
		}

		/**
		 * 根据 KEY,field 储蓄 对应的 Serializable 的值
		 * @param <T>
		 * @param key
		 * @param fieldName
		 * @return
		 */
		public <T> boolean setMapByFields(final String key, final String fieldName, final Object object) {
			byte[] serializableToBytes = Bytes.objectToBytes(object);
			return set(key, Bytes.stringToUtf8Bytes(fieldName), serializableToBytes, null);
		}

		/**
		 * 设置Key对应的Map以字符串的形式
		 */
		public boolean multiSetStringMap(final String namespace, final String key, final Map<String, String> map,
				final Integer expireInSecs) {
			String redisKey = getFullKey(namespace, key);
			return multiSetStringMap(redisKey, map, expireInSecs);
		}

		public boolean multiSetStringMap(final String key, final Map<String, String> map, final Integer expireInSecs) {
			Map<byte[], byte[]> mapBytes = Bytes.utf8StringMapToByteMap(map);
			return muiltSet(key, mapBytes, expireInSecs);
		}

		/**
		 * 取值 KEY对应Map以字符串的形式
		 */
		public Map<String, String> getStringMap(final String namespace, final String key) {
			String redisKey = getFullKey(namespace, key);
			return getStringMap(redisKey);
		}
		public Map<String, String> getStringMap(final String key) {
			Map<byte[], byte[]> mapByte = getAll(key);
			return Bytes.byteMapToUTF8StringMap(mapByte);
		}

		/**
		 * 以field的字段的形式获取Map
		 */
		public Map<String, String> getStringMapByFields(final String namespace, final String key,
				final String... fieldNames) {
			if (null == fieldNames) {
				return null;
			}
			String rkey = getFullKey(namespace, key);
			return getStrMapByFields(rkey, fieldNames);
		}
		public Map<String, String> getStrMapByFields(final String key,
				final String... fieldNames) {
			if (null == fieldNames) {
				return null;
			}
			byte[][] stringToByte = Bytes.stringsToBytes(fieldNames);
			Map<byte[], byte[]> hashMuiltGet = muiltGet(key, stringToByte);
			Map<String, String> mapByteToString = Bytes.byteMapToUTF8StringMap(hashMuiltGet);

			if (MapUtils.isNotEmpty(mapByteToString)) {
				return mapByteToString;
			}
			return null;
		}

		/**
		 * 以field的字段的形式 存储Map
		 */
		public boolean setStringMapByFields(final String namespace, final String key, final String fieldName,
				final String value, final Integer expireInSecs) {
			if (null == fieldName || key == null) {
				return false;
			}
			String rkey = getFullKey(namespace, key);
			return setStringMapByFields(rkey, fieldName, value, expireInSecs);
		}
		public boolean setStringMapByFields(final String key, final String fieldName,
				final String value, final Integer expireInSecs) {
			if (null == fieldName || key == null) {
				return false;
			}
			byte[] fieldNameByte = Bytes.stringToUtf8Bytes(fieldName);
			byte[] valueByte = Bytes.stringToUtf8Bytes(value);

			return set(key, fieldNameByte, valueByte, expireInSecs);

		}
	}

	/**
	 * REDIS List 操作工具类
	 */
	public static class OptList extends ListRedisTemplate {

		private static final class InstanceHolder {
			private static final OptList INSTANCE = new OptList();
		}

		protected static OptList getInstance() {
			return InstanceHolder.INSTANCE;
		}

		private OptList() {

		}

		/**
		 * 储存 list 对象
		 */
		public long putJsonArray(final String namespace, final String key, final List<? extends Object> list , final int expire, final TimeUnit timeUnit) {
			if (CollectionUtils.isEmpty(list)) {
				return 0;
			}
			String fullKey = getFullKey(namespace, key);
			byte[][] strByte = Bytes.objectListToByteArray(list);
			long rightPush = rightPush(fullKey, strByte);
			int expireInSecs = getTimes(expire, timeUnit);
			expireBySeconds(fullKey, expireInSecs);
			return rightPush;
		}

		/**
		 * 获取整个list 对象
		 */
		public List<? extends Object> getJsonArray(final String namespace, final String key) {
			String fullKey = getFullKey(namespace, key);
			List<byte[]> bySubscript = getBySubScript(fullKey, 0, -1);
			return Bytes.byteListToObjectList(bySubscript);
		}
	}

	/**
	 * REDIS SET操作的工具类
	 */
	public static class OptSet extends SetRedisTemplate {

		private static final class InstanceHolder {
			private static final OptSet INSTANCE = new OptSet();
		}

		protected static OptSet getInstance() {
			return InstanceHolder.INSTANCE;
		}

		private OptSet() {

		}
	}

	/**
	 * REDIS ZSet操作工具类
	 */
	public static class OptZSet extends SortedSetRedisTemplate {

		private static final class InstanceHolder {
			private static final OptZSet INSTANCE = new OptZSet();
		}

		public static OptZSet getInstance() {
			return InstanceHolder.INSTANCE;
		}

		private OptZSet() {

		}
	}

	/**
	 * REDIS发布订阅操作工具
	 */
	public static class OptPubSub extends PubSubRedisTemplate {

		private static final class InstanceHolder {
			private static final OptPubSub INSTANCE = new OptPubSub();
		}

		protected static OptPubSub getInstance() {
			return InstanceHolder.INSTANCE;
		}

		private OptPubSub() {

		}
	}

	/**
	 * 获取REDIS KEY的 完整名称
	 */
	public static String getFullKey(final String namespace, final String key) {
		String result = "";
		if (StringUtils.isNotBlank(namespace)) {
			result = result + namespace;
		}
		if (StringUtils.isNotBlank(key)) {
			result = result + ":" + key;
		}
		return result;
	}

	/**
	 * 获取REDIS的过期时间，统一转化单位为秒
	 */
	public static Integer getTimes(Integer expire, TimeUnit timeUnit) {
		Long expireTimeInSecs = TimeUnit.SECONDS.convert(expire, timeUnit);
		return expireTimeInSecs.intValue();
	}

	/**
	 * 获取REDIS的过期时间，统一转化单位为秒
	 */
	public static Integer getTimes(Long expire, TimeUnit timeUnit) {
		Long expireTimeInSecs = TimeUnit.SECONDS.convert(expire, timeUnit);
		return expireTimeInSecs.intValue();
	}

}
