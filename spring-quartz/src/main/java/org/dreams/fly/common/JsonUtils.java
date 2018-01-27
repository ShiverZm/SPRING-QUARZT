package org.dreams.fly.common;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.type.JavaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Collection;

public class JsonUtils {

	private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	static{
		// 设置输入时忽略在JSON字符串中存在但Java对象实际没有的属性
		OBJECT_MAPPER.disable(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
		OBJECT_MAPPER.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
		OBJECT_MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
		OBJECT_MAPPER.getSerializerProvider().setNullValueSerializer(new JsonSerializer<Object>() {
            @Override
            public void serialize(Object value, JsonGenerator jgen,
                    SerializerProvider provider) throws IOException,
                    JsonProcessingException {
                jgen.writeString("");
            }
        });
	}

	public static String toJson(Object obj) {
		StringWriter writer = new StringWriter();
		JsonGenerator gen = null;
		try {
			gen = new JsonFactory().createJsonGenerator(writer);
			OBJECT_MAPPER.writeValue(gen, obj);
			gen.close();
			String json = writer.toString();
			writer.close();
			return json;
		} catch (IOException e) {
			LOG.error("occur error:", e);
		}

		return null;
	}

	public static <T> T fromJson(String json, Class<T> clazz) {
		T object = null;
		try {
			object = OBJECT_MAPPER.readValue(json, clazz);
		} catch (Exception e) {
			LOG.error("occur error:", e);
		}
		return object;
	}

	public static JavaType assignList(@SuppressWarnings("rawtypes") Class<? extends Collection> collection, Class<? extends Object> object) {
		return OBJECT_MAPPER.getTypeFactory().constructParametricType(collection, object);
	}

	public static <T> Collection<T> readValuesAsArrayList(String key, Class<T> object) {
		Collection<T> result = null;
		try {
			result = OBJECT_MAPPER.readValue(key, assignList(Collection.class, object));
		} catch (Exception e) {
			LOG.warn("occur error:", e);
		}
		return result;
	}

	public static boolean containField(String json, String field) throws Exception {
	    JsonNode node = OBJECT_MAPPER.readTree(json);
	    return node.get(field) != null;
	}

}