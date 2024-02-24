package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;
import java.nio.charset.Charset;

import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Test;


public class StringToHexSerializerTest {
	private static final String TEST_STRING = "Test123456String";
	private static final String TEST_HEX = SerializationUtils.bytesToHex(TEST_STRING.getBytes(Charset.forName("UTF-16")));
	
	@Test
	public void test_Serialize_String() throws XMLStreamException {
		final StringToHexSerializer<String> toHexSerializer = new StringToHexSerializer<String>(String::valueOf,String::valueOf);
		final String[] expectedValue=new String[1];
		toHexSerializer.serialize(TEST_STRING, e->expectedValue[0]=e);
		Assert.assertEquals(TEST_HEX, expectedValue[0]);
	}
	
	@Test
	public void test_Deserialize_String() throws IOException {
		final StringToHexSerializer<String> toHexSerializer = new StringToHexSerializer<String>(String::valueOf,String::valueOf);
		XMLContentConverter<String> deserializer = toHexSerializer.newDeserializer();
		String value = deserializer.process(TEST_HEX);
		Assert.assertEquals(TEST_STRING,value);
	}
	
}
