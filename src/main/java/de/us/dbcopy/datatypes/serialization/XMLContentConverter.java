package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;

import de.us.dbcopy.datatypes.XMLContentProcessor;

@FunctionalInterface
public interface XMLContentConverter<T> extends AutoCloseable {
	
	/**
	 * Deserializes the given xmlContent and create a value for it. May be called multiple times.
	 *  
	 * @param xmlContent
	 * @return
	 * @throws IOException
	 */
	public T process(String xmlContent) throws IOException;
	
	/**
	 * Called by the {@link XMLContentProcessor} after all content has been processed.
	 * This means that the {@link #process(String)} method of this object should no longer be called
	 * and resources used by this object can be freed.
	 * 
	 */
	public default void deserializationComplete() throws IOException {
	}
	
	/**
	 * Called by the {@link XMLContentProcessor} to obtain a {@link DeserializationValue} for the given xmlContent.
	 * Unless overwritten this method creates a {@link DeserializationValue}
	 * which is based on the value returned by {@link #process(String)} and {@link DeserializationValue#isNewValue()}
	 * is false, which means that {@link #process(String)} must return the same instance of the object.
	 * @param xmlContent The content to create the {@link DeserializationValue} for
	 * @throws IOException Thrown usually if {@link #process(String)} throws such exception
	 */
	public default DeserializationValue<T> getDeserializationValue(String xmlContent) throws IOException {
		return new DeserializationValue<T>(process(xmlContent), false);
	}
	
	/**
	 * Closes this {@link XMLContentConverter} which means that no resources of this object are required anymore.
	 */
	@Override
	public default void close() throws IOException {
		
	}
}
