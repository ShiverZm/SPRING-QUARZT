package org.dreams.fly.common.bytes;

import  org.dreams.fly.common.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;


public final class Bytes {
	
	private static final Logger LOG = LoggerFactory.getLogger(Bytes.class);

	private Bytes() {

	}

	/**
	 * Map<String, Object> ==> Map<byte[], byte[]>
	 */
	public static Map<byte[], byte[]> stringObjectMapToByteMap(Map<String, ? extends Object> map) {
		Map<byte[], byte[]> mapBytes = new HashMap<byte[], byte[]>();
		for (String key : map.keySet()) {
			mapBytes.put(stringToUtf8Bytes(key), objectToBytes(map.get(key)));
		}
		return mapBytes;
	}

	/**
	 * Map<byte[], byte[]> ==> Map<String, Object>
	 */
	public static Map<String, ? extends Object> byteMapToStringObjectMap(Map<byte[], byte[]> map) {
		Map<String, Object> mapBytes = new HashMap<String, Object>();
		for (byte[] key : map.keySet()) {
			mapBytes.put(bytesToUtf8String(key), bytesToObject(map.get(key)));
		}
		return mapBytes;
	}

	/**
	 * string数组 转换为 二维字节数组(byte)
	 * 
	 * @param strs
	 * @return
	 */
	public static byte[][] stringsToBytes(String... strs) {
		byte[][] bytes = new byte[strs.length][];
		/*
		int i = 0;
		for (String str : strs) {
			bytes[i] = stringToUtf8Bytes(str);
			i++;
		}
		*/
		int size = strs.length;
		for(int i = 0; i < size; i++){
			bytes[i] = stringToUtf8Bytes(strs[i]);
			
		}
		return bytes;
	}

	/**
	 * List<byte[]> 转换为 List<String>
	 * 
	 * @param listbytes
	 * @return
	 */
	public static List<String> listBytesToListString(List<byte[]> listbytes) {
		List<String> listStr = new ArrayList<String>();
		int size = listbytes.size();
		for(int i = 0; i < size; i++){
			byte[] bs = listbytes.get(i);
			listStr.add(bytesToUtf8String(bs));
		}
		
		
		return listStr;
	}

	/**
	 * Set<byte[]> 转换为 Set<String>
	 * 
	 * @param setByte
	 * @return
	 */
	public static Set<String> setBytestoSetString(Set<byte[]> setByte) {
		Set<String> setStrs = new HashSet<String>();
		Iterator<byte[]>  iter =  setByte.iterator();
		while(iter.hasNext()){
			setStrs.add(bytesToUtf8String(iter.next()));
		}
		return setStrs;
	}

	
	
	/**
	 * Boolean 转换成 字节数组
	 */
	public static byte[] boolToBytes(boolean boolVal) {
		return new byte[] { boolVal ? (byte) 1 : 0 };
	}

	/**
	 * 字节数组 转换成 boolean
	 */
	public static boolean bytesToBool(byte[] bytes) {
		if (null != bytes && bytes.length == 1) {
			return bytes[0] == 0 ? false : true;
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	public static byte[] byteToBytes(byte byteVal) {
		return new byte[] { byteVal };
	}

	public static byte bytesToByte(byte[] bytes) {
		if (null != bytes && bytes.length == 1) {
			return bytes[0];
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	public static byte[] charToBytes(char charVal) {
		return new byte[] { (byte) (charVal >> 8), (byte) charVal };
	}

	public static char bytesToChar(byte[] bytes) {
		if (null != bytes && bytes.length == 2) {
			return (char) (bytes[0] << 8 | bytes[1] & 0XFF);
		} else if (null != bytes && bytes.length < 2) {
			byte[] newBytes = { 0, 0 };
			for (int i = 0; i < bytes.length; i++) {
				newBytes[i] = bytes[i];
			}
			return (char) (newBytes[0] << 8 | newBytes[1] & 0XFF);
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	public static byte[] shortToBytes(short shortVal) {
		return new byte[] { (byte) (shortVal >> 8), (byte) shortVal };
	}

	public static short bytesToShort(byte[] bytes) {
		if (null != bytes && bytes.length == 2) {
			return (short) (bytes[0] << 8 | bytes[1] & 0XFF);
		} else if (null != bytes && bytes.length < 2) {
			byte[] newBytes = { 0, 0 };
			for (int i = 0; i < bytes.length; i++) {
				newBytes[i] = bytes[i];
			}
			return (short) (newBytes[0] << 8 | newBytes[1] & 0XFF);
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	public static byte[] intToBytes(int intVal) {
		return new byte[] { (byte) (intVal >> 24), (byte) (intVal >> 16), (byte) (intVal >> 8), (byte) intVal };
	}

	public static int bytesToInt(byte[] bytes) {
		if (null != bytes && bytes.length == 4) {
			return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
		} else if (null != bytes && bytes.length < 4) {
			byte[] newBytes = { 0, 0, 0, 0 };
			for (int i = 0; i < bytes.length; i++) {
				newBytes[i] = bytes[i];
			}
			return newBytes[0] << 24 | (newBytes[1] & 0xFF) << 16 | (newBytes[2] & 0xFF) << 8 | (newBytes[3] & 0xFF);
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	public static byte[] longToBytes(long longVal) {
		return new byte[] { (byte) (longVal >> 56), (byte) (longVal >> 48), (byte) (longVal >> 40),
				(byte) (longVal >> 32), (byte) (longVal >> 24), (byte) (longVal >> 16), (byte) (longVal >> 8),
				(byte) longVal };
	}

	public static long bytesToLong(byte[] bytes) {
		if (null != bytes && bytes.length == 8) {
			return ((((long) bytes[0] & 0xFF) << 56) | (((long) bytes[1] & 0XFF) << 48)
					| ((long) (bytes[2] & 0XFF) << 40) | (((long) bytes[3] & 0XFF) << 32)
					| (((long) bytes[4] & 0XFF) << 24) | (((long) bytes[5] & 0XFF) << 16)
					| (((long) bytes[6] & 0XFF) << 8) | ((bytes[7] & 0XFF)));
		} else if (null != bytes && bytes.length < 8) {
			byte[] newBytes = { 0, 0, 0, 0, 0, 0, 0, 0 };
			for (int i = 0; i < bytes.length; i++) {
				newBytes[i] = bytes[i];
			}
			return ((((long) newBytes[0] & 0xFF) << 56) | (((long) newBytes[1] & 0XFF) << 48)
					| ((long) (newBytes[2] & 0XFF) << 40) | (((long) newBytes[3] & 0XFF) << 32)
					| (((long) newBytes[4] & 0XFF) << 24) | (((long) newBytes[5] & 0XFF) << 16)
					| (((long) newBytes[6] & 0XFF) << 8) | ((newBytes[7] & 0XFF)));
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	public static byte[] floatToBytes(float floatVal) {
		int iv = Float.floatToRawIntBits(floatVal);
		return new byte[] { (byte) ((iv >> 24) & 0XFF), (byte) ((iv >> 16) & 0XFF), (byte) ((iv >> 8) & 0XFF),
				(byte) ((iv) & 0XFF) };
	}

	public static float bytesToFloat(byte[] bytes) {
		if (null != bytes && bytes.length == 4) {
			int iv = bytes[0] << 24 | (bytes[1] & 0XFF) << 16 | (bytes[2] & 0XFF) << 8 | bytes[3];
			return Float.intBitsToFloat(iv);
		} else if (null != bytes && bytes.length < 4) {
			byte[] newBytes = { 0, 0, 0, 0 };
			for (int i = 0; i < bytes.length; i++) {
				newBytes[i] = bytes[i];
			}
			int iv = newBytes[0] << 24 | (newBytes[1] & 0XFF) << 16 | (newBytes[2] & 0XFF) << 8 | newBytes[3];
			return Float.intBitsToFloat(iv);
		} else {
			throw new RuntimeException("bytes length incorrect!");
		}
	}

	// 浮点到字节转换
	public static byte[] doubleToBytes(double d) {
		byte writeBuffer[] = new byte[8];
		long v = Double.doubleToLongBits(d);
		writeBuffer[0] = (byte) (v >>> 56);
		writeBuffer[1] = (byte) (v >>> 48);
		writeBuffer[2] = (byte) (v >>> 40);
		writeBuffer[3] = (byte) (v >>> 32);
		writeBuffer[4] = (byte) (v >>> 24);
		writeBuffer[5] = (byte) (v >>> 16);
		writeBuffer[6] = (byte) (v >>> 8);
		writeBuffer[7] = (byte) (v >>> 0);
		return writeBuffer;

	}

	// 字节到浮点转换
	public static double bytesToDouble(byte[] readBuffer) {
		return Double.longBitsToDouble((((long) readBuffer[0] << 56) + ((long) (readBuffer[1] & 255) << 48)
				+ ((long) (readBuffer[2] & 255) << 40) + ((long) (readBuffer[3] & 255) << 32)
				+ ((long) (readBuffer[4] & 255) << 24) + ((readBuffer[5] & 255) << 16) + ((readBuffer[6] & 255) << 8)
				+ ((readBuffer[7] & 255) << 0)));
	}

	public static byte[] stringToBytes(String stringVal, String charsetName) {
		if (null == stringVal) {
			return new byte[] {};
		}
		try {
			return stringVal.getBytes(charsetName);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static byte[] stringToUtf8Bytes(String stringVal) {
		return stringToBytes(stringVal, "UTF-8");
	}

	public static String bytesToString(byte[] bytes, String charsetName) {
		if (null == bytes) {
			return null;
		}
		try {
			return new String(bytes, charsetName);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String bytesToUtf8String(byte[] bytes) {
		return bytesToString(bytes, "UTF-8");
	}

	public static byte[] objectToBytes(Object obj) {
		return MarshallingUtis.serialize(obj);
	}

	public static Object bytesToObject(byte[] bytes) {
		return MarshallingUtis.deserialize(bytes);
	}

	public static byte[] objectToJsonBytes(Object obj) {
		try {
			return JsonUtils.toJson(obj).getBytes("UTF-8");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static <T> T jsonBytesToObject(byte[] bytes, Class<T> clazz) {
		try {
			return JsonUtils.fromJson(new String(bytes, "UTF-8"), clazz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> Collection<T> jsonBytesToObjectCollection(byte[] bytes, Class<T> clazz) {
		try {
			return JsonUtils.readValuesAsArrayList(new String(bytes, "UTF-8"), clazz);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 字节list转换为字符串list
	 */
	public static List<String> byteListToUtf8StringList(List<byte[]> byteList) {
		List<String> result = new ArrayList<String>();
		for (byte[] b : byteList) {
			result.add(bytesToUtf8String(b));
		}
		return result;
	}

	public static byte[][] objectListToByteArray(List<? extends Object> list) {
		byte[][] result = new byte[list.size()][];
		int i = 0;
		for (Object object : list) {
			result[i] = objectToBytes(object);
			i++;
		}
		return result;
	}

	public static List<Object> byteListToObjectList(List<byte[]> list) {
		List<Object> result = new ArrayList<Object>();
		for (byte[] bs : list) {
			result.add(bytesToObject(bs));
		}
		return result;
	}

	/**
	 * 将map<String,String> 转化成 Map<byte[], byte[]>
	 * 
	 * @param map
	 * @return
	 */
	public static Map<byte[], byte[]> utf8StringMapToByteMap(Map<String, String> map) {
		Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
		Map<byte[], byte[]> byteMap = new HashMap<byte[], byte[]>();
		while (it.hasNext()) {
			Map.Entry<String, String> entry = it.next();
			try {
				if(null != entry && entry.getKey() != null && entry.getValue() != null){
					byteMap.put(entry.getKey().getBytes("UTF-8"), entry.getValue().getBytes("UTF-8"));
				}
			} catch (UnsupportedEncodingException e) {
				LOG.error("occur error:", e);
			}
		}
		return byteMap;
	}

	public static Map<String, String> byteMapToUTF8StringMap(Map<byte[], byte[]> map) {
		Map<String, String> result = new HashMap<String, String>();
		for (byte[] keyByte : map.keySet()) {
			result.put(bytesToUtf8String(keyByte), bytesToUtf8String(map.get(keyByte)));
		}
		return result;
	}
}
