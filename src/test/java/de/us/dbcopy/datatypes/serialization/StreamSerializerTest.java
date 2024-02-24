package de.us.dbcopy.datatypes.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Test;

public class StreamSerializerTest {
	private static final byte[] TEST_BYTES = new byte[] {10,55,77,-13,-88,4,127,-87,17,-74};
	
	private static final ByteArrayInputStream TEST_STREAM = new ByteArrayInputStream(TEST_BYTES);
	
	private static final String TEST_HEX = SerializationUtils.bytesToHex(TEST_BYTES);
	
	@Test
	public void test_Serialize_Stream_Success() throws XMLStreamException, IOException {
		StreamSerializer serializer = new StreamSerializer();
		final String[] expectedValue = new String[1];
		serializer.serialize(TEST_STREAM, e->expectedValue[0]=e);
		Assert.assertEquals(TEST_HEX,expectedValue[0]);
	}
	

	@Test
	public void test_Deserialize_Stream_Success() throws XMLStreamException, IOException {
		StreamSerializer serializer = new StreamSerializer();
		XMLContentConverter<InputStream> deserializer = serializer.newDeserializer();
		DeserializationValue<InputStream> process1 = deserializer.getDeserializationValue(TEST_HEX.substring(0, 1));
		DeserializationValue<InputStream> process2 = deserializer.getDeserializationValue(TEST_HEX.substring(1));
		Assert.assertSame(process1.getValue(), process2.getValue());
		InputStream value = process1.getValue();
		byte[] b=new byte[TEST_BYTES.length];
		value.read(b);
		Assert.assertArrayEquals(TEST_BYTES, b);
		Assert.assertEquals(-1, value.read());
	}

}
