package de.us.dbcopy.datatypes.serialization;

import javax.xml.stream.XMLStreamException;

/**
 * {@link XMLSerializer} that uses the {@link Object#toString()} to serialize values.
 * A custom {@link XMLContentConverter} is used to deserialize these values.
 * @param <T> The type of the value that is serialized
 */
public class StringSerializer<T> implements XMLSerializer<T> {
	
	private final XMLContentConverter<T> deserialize;
	
	public StringSerializer(final XMLContentConverter<T> deserializer) {
		deserialize = deserializer;
		
	}
	
	@Override
	public void serialize(T value, XMLContentCrafter consumer) throws XMLStreamException {
		consumer.craft(value.toString());
	}

	@Override
	public XMLContentConverter<T> newDeserializer() {
		return this.deserialize;
	}
}
