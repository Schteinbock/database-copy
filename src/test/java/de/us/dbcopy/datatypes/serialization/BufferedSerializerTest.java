package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Test;

public class BufferedSerializerTest {
	
	private static final byte[] TEST_BYTES = new byte[] {10,55,77,-13,-88,4,127,-87,17,-74};
	
	private static final String TEST_HEX = SerializationUtils.bytesToHex(TEST_BYTES);
	
	@Test
	public void test_Buffered_Serializer_Serialization_Works_As_Expected() throws XMLStreamException, IOException {
		byte[] buffer=new byte[10];
		BufferedSerializer<byte[]> serializer = new BufferedSerializer<byte[]>(10, ((bb,v)->bb.put(v)), (bb)->{bb.get(buffer);return buffer;});
		final String[] hexString = new String[1];
		serializer.serialize(TEST_BYTES, e->hexString[0]=e);
		Assert.assertEquals(hexString[0], TEST_HEX);
	}
	
	@Test
	public void test_Buffered_Serializer_Deserialization_Works_As_Expected() throws XMLStreamException, IOException {
		byte[] buffer=new byte[10];
		BufferedSerializer<byte[]> serializer = new BufferedSerializer<byte[]>(10, ((bb,v)->bb.put(v)), (bb)->{bb.get(buffer);return buffer;});
		XMLContentConverter<byte[]> xmlContentConverter = serializer.newDeserializer();
		byte[] process = xmlContentConverter.process(TEST_HEX);
		Assert.assertArrayEquals(process,TEST_BYTES);
	}
}
