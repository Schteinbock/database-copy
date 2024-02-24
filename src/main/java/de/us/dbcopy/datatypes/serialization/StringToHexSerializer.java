package de.us.dbcopy.datatypes.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.function.Function;

import javax.xml.stream.XMLStreamException;

/**
 * {@link XMLSerializer} That processes String values of the given type and converts a String representation 
 * @param <T> The type of the value that is processed
 * @author Uli Schneider
 */
public class StringToHexSerializer<T> implements XMLSerializer<T> {
	
	private final Function<? super T,String> toStringFunction;
	
	private final Function<String,T> deserializer;

	
	public StringToHexSerializer(Function<? super T, String> toString,Function<String,T> deserializer) {
		this.toStringFunction = toString;
		this.deserializer = deserializer;
	}
	
	@Override
	public void serialize(T value, XMLContentCrafter consumer) throws XMLStreamException {
		String string = toStringFunction.apply(value);
		consumer.craft(SerializationUtils.bytesToHex(string.getBytes(Charset.forName("UTF-16"))));
	}

	@Override
	public XMLContentConverter<T> newDeserializer() {
		return new XMLContentConverter<T>() {
			
			private ByteArrayOutputStream bos=null;
			
			@Override
			public T process(String xmlContent) throws IOException {
				if(this.bos==null) {
					this.bos=new ByteArrayOutputStream(xmlContent.length()/2);
				}
				this.bos.write(SerializationUtils.hexToBytes(xmlContent));
				return StringToHexSerializer.this.deserializer.apply(new String(this.bos.toByteArray(),Charset.forName("UTF-16")));
			}
			
			@Override
			public DeserializationValue<T> getDeserializationValue(String xmlContent) throws IOException {
				return new DeserializationValue<T>(process(xmlContent), true);
			}
			
			@Override
			public void deserializationComplete() {
				try {
					if(this.bos!=null) {
						this.bos.close();
					}
				} catch (IOException e) {
					// NOOP
				}
				this.bos=null;
			}
		};
	}
	
}
