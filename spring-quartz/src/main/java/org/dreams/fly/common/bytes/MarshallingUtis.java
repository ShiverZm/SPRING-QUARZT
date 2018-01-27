package org.dreams.fly.common.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jboss.marshalling.ExceptionListener;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.SimpleClassResolver;
import org.jboss.marshalling.Unmarshaller;

public class MarshallingUtis {

    private final static MarshallerFactory MARSHALLER_FACTORY;
    private final static MarshallingConfiguration CONFIGURATION;

    static {
    	MARSHALLER_FACTORY = Marshalling.getProvidedMarshallerFactory("river");
        if (MARSHALLER_FACTORY == null) {
            throw new IllegalStateException("Unable to create marshaller factory");
        }
        CONFIGURATION = new MarshallingConfiguration();
        CONFIGURATION.setVersion(3);
        CONFIGURATION.setClassCount(10);
        CONFIGURATION.setBufferSize(8096);
        CONFIGURATION.setInstanceCount(100);
        CONFIGURATION.setExceptionListener(new MarshallingException());
        CONFIGURATION.setClassResolver(new SimpleClassResolver(MarshallingUtis.class.getClassLoader()));
    }

    private static Marshaller createMarshaller() throws IOException {
        return MARSHALLER_FACTORY.createMarshaller(CONFIGURATION);
    }

    private static Unmarshaller createUnmarshaller() throws IOException {
        return MARSHALLER_FACTORY.createUnmarshaller(CONFIGURATION);
    }

    public static byte[] serialize(Object object){
    	byte[] result = null;
    	try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serialize(object,baos);
			result = baos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    	return result;
    }

    public static Object deserialize(byte[] bytes){
    	Object result = null;
    	try {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			result = deserialize(bais);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    	return result;
    }

	public static <T extends Object> T deserialize(byte[] bytes, Class<T> clazz){
    	T result = null;
    	try {
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			result = clazz.cast(deserialize(bais));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    	return result;
    }

    protected static void serialize(Object object, OutputStream outputStream) throws IOException {
        Marshaller marshaller = createMarshaller();
        marshaller.start(Marshalling.createByteOutput(outputStream));
        try {
            marshaller.writeObject(object);
        } finally {
            marshaller.finish();
        }
    }

    protected static Object deserialize(InputStream inputStream) throws Exception {
        Unmarshaller unmarshaller = createUnmarshaller();
        unmarshaller.start(Marshalling.createByteInput(inputStream));
        try {
            return unmarshaller.readObject();
        } finally {
            unmarshaller.finish();
        }
    }

	protected static <T extends Object> T deserialize(InputStream inputStream, Class<T> type) throws Exception {
        Unmarshaller unmarshaller = createUnmarshaller();
        unmarshaller.start(Marshalling.createByteInput(inputStream));
        try {
            return type.cast(unmarshaller.readObject());
        } finally {
            unmarshaller.finish();
        }
    }

    private static class MarshallingException implements ExceptionListener {

        @Override
        public void handleMarshallingException(Throwable problem, Object subject) {
            throw new RuntimeException(String.format("Unable to marshall object %s", subject.getClass().getName()), problem); //$NON-NLS-1$
        }

        @Override
        public void handleUnmarshallingException(Throwable problem, Class<?> subjectClass) {
            throw new RuntimeException(String.format("Unable to unmarshall object %s", subjectClass), problem); //$NON-NLS-1$
        }

        @Override
        public void handleUnmarshallingException(Throwable problem) {
            throw new RuntimeException("Unable to unmarshall object", problem); //$NON-NLS-1$
        }

    }

}