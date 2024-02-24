package de.us.dbcopy.datatypes.serialization;
/**
 * Class that encapsulates a deserialization value and a boolean value that indicates
 * whether the given value is new.
 * @author Uli Schneider
 */
public class DeserializationValue<T> {
	
	private final boolean newValue;
	
	private final T value;
	
	
	DeserializationValue(T value,boolean newValue) {
		this.value = value;
		this.newValue = newValue;
	}
	
	public boolean isNewValue() {
		return this.newValue;
	}
	
	public T getValue() {
		return this.value;
	}
}
