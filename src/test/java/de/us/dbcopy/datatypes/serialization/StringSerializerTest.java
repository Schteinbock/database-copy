package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Test;

public class StringSerializerTest {
	
	private static final String TEST_STRING="Hallo1234567";
	
	@Test
	public void test_String_Serializer_Success() throws XMLStreamException {
		StringSerializer<String> stringSerializer = new StringSerializer<String>(String::valueOf);
		final String[] expectedValue=new String[1];
		stringSerializer.serialize(TEST_STRING, e->expectedValue[0]=e);
		Assert.assertEquals(TEST_STRING, expectedValue[0]);
	}
	
	@Test
	public void test_String_Serializer_Deserialize_Correctly() throws IOException {
		StringSerializer<String> stringSerializer = new StringSerializer<String>(String::valueOf);
		XMLContentConverter<String> deserializer = stringSerializer.newDeserializer();
		String value = deserializer.process(TEST_STRING);
		Assert.assertEquals(TEST_STRING, value);
	}
	
}
