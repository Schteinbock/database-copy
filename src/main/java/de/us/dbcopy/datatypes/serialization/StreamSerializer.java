package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.xml.stream.XMLStreamException;

/**
 * {@link XMLSerializer} that converts stream values.
 * @author Uli Schneider
 */
public class StreamSerializer implements XMLSerializer<InputStream> {
	
	private static final int BUFFER_SIZE = 100000;
	
	@Override
	public void serialize(InputStream value, XMLContentCrafter consumer) throws XMLStreamException, IOException {
		InputStream inputStream = value;
		byte[] bytes = new byte[BUFFER_SIZE];
		int sz=0;
		while((sz=inputStream.read(bytes))!=-1) {
			consumer.craft(SerializationUtils.bytesToHex(bytes,sz));
		}
	}

	@Override
	public XMLContentConverter<InputStream> newDeserializer() {
		return new StreamContentConverter();
	}

	private static final class StreamContentConverter implements XMLContentConverter<InputStream> {
		private Character truncatedChar=null;
		private InputStream inputStream=null;
		private OutputStream os =null;
	
		@Override
		public InputStream process(String xmlContent) throws IOException {
			if(this.inputStream==null) {
				final Path blobFile = Files.createTempFile(null,null);
				this.inputStream=Files.newInputStream(blobFile);
				this.os=Files.newOutputStream(blobFile,StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
			}
			if(this.truncatedChar!=null) {
				xmlContent=this.truncatedChar+xmlContent;
				this.truncatedChar=null;
			}
			int length=xmlContent.length();
			if((length%2)!=0) {
				this.truncatedChar=xmlContent.charAt(length-1);
			}
			this.os.write(SerializationUtils.hexToBytes(xmlContent));
			return this.inputStream;
		}
	
		@Override
		public void deserializationComplete() throws IOException {
			if(this.os!=null) {
				this.os.close();
			}
		}
	
		@Override
		public void close() throws IOException {
			if(this.inputStream!=null) {
				this.inputStream.close();
			}
		}

	}

}