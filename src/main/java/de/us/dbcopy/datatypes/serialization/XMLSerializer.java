package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import de.us.dbcopy.datatypes.JavaDataType;

/**
 * Interface used to serialize and deserialize SQL values. Each {@link JavaDataType} has its own 
 * instance of this interface.
 * @param <T> The Java value this {@link XMLSerializer} serializes and deserializes
 * @author Uli Schneider
 */
public interface XMLSerializer<T> {
	
	
	/**
	 * Serialize the given value reporting its string to the provided {@link XMLContentConverter}.
	 * @throws XMLStreamException If {@link XMLContentCrafter#craft(String)} throws an {@link XMLStreamException}
	 * @throws IOException
	 */
	public void serialize(T value,XMLContentCrafter consumer) throws XMLStreamException,IOException;
	
	/**
	 * Get a new {@link XMLContentConverter} which internally converts a string to the Java value.
	 * 
	 */
	public XMLContentConverter<T> newDeserializer();
	
	

}
