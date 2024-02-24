package de.us.dbcopy.datatypes.serialization;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;

import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.Test;

public class ReaderSerializerTest {
	private static final String TEST_STRING_VALUE = "Test123456789";
	
	private static final String TEST_HEX_VALUE = SerializationUtils.bytesToHex(TEST_STRING_VALUE.getBytes(Charset.forName("UTF-16")));
	
	@Test
	public void test_Reader_Serialization_Works_As_Expected() throws XMLStreamException, IOException {
		ReaderSerializer readerSerializer = new ReaderSerializer();
		StringReader stringReader = new StringReader(TEST_STRING_VALUE);
		final String[] value = new String[1];
		readerSerializer.serialize(stringReader, e->value[0]=e);
		Assert.assertEquals(TEST_HEX_VALUE,value[0]);
	}
	
	@Test
	public void test_Reader_Deserialization_Works_As_Expected() throws IOException {
		ReaderSerializer readerSerializer = new ReaderSerializer();
		XMLContentConverter<Reader> deserializer = readerSerializer.newDeserializer();
		DeserializationValue<Reader> process1 = deserializer.getDeserializationValue(TEST_HEX_VALUE.substring(0,1));
		DeserializationValue<Reader> process2 = deserializer.getDeserializationValue(TEST_HEX_VALUE.substring(1));
		Reader value = process1.getValue();
		Assert.assertSame(value, process2.getValue());
		Assert.assertFalse(process1.isNewValue());
		Assert.assertFalse(process2.isNewValue());
		deserializer.deserializationComplete();
		CharArrayWriter writer = new CharArrayWriter();
		int c;
		while((c=value.read())!=-1) {
			writer.write(c);
		}
		deserializer.close();
		writer.toCharArray();
		Assert.assertTrue(TEST_STRING_VALUE.equals(writer.toString()));
	}
}
