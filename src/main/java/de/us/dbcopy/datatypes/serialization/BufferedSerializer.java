package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.xml.stream.XMLStreamException;


/**
 * Serializer using an internal {@link ByteBuffer} to serialize and deserialize values 
 * @param <T> The Java type of this {@link BufferedSerializer}, for example {@link Float}
 * @author Uli Schneider
 */
public class BufferedSerializer<T> implements XMLSerializer<T> {
	private ByteBuffer buffer;
	
	private final BiConsumer<ByteBuffer,T> consumer;
	
	private final Function<ByteBuffer,T> deserializer;
	
	private final Object syncObject = new Object();
	
	/**
	 * @param size The size of the {@link ByteBuffer} to use
	 * @param consumer The function that puts values into the {@link ByteBuffer},  for example {@link ByteBuffer#putFloat(float)}
	 * @param deserializer The function that reads values from the {@link ByteBuffer}, for example {@link ByteBuffer#getFloat()}
	 */
	public BufferedSerializer(int size,BiConsumer<ByteBuffer, T> consumer,Function<ByteBuffer,T> deserializer) {
		this.consumer = consumer;
		this.deserializer = deserializer;
		this.buffer=ByteBuffer.allocate(size);
	}
	
	@Override
	public void serialize(T value, XMLContentCrafter consumer) throws XMLStreamException, IOException {
		synchronized (this.syncObject) {
			this.consumer.accept(this.buffer, value);
			byte[] buf = this.buffer.array();
			consumer.craft(SerializationUtils.bytesToHex(buf,buf.length));
			this.buffer.clear();
		}
	}
	
	@Override
	public XMLContentConverter<T> newDeserializer() {
		return string-> {
			final T value;
			synchronized (BufferedSerializer.this.syncObject) {
				this.buffer.clear();
				byte[] hex = SerializationUtils.hexToBytes(string);
				this.buffer.put(hex);
				this.buffer.position(0);
				value = this.deserializer.apply(this.buffer);
			}
			return value;
		};
	}
}
