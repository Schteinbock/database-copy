package de.us.dbcopy.datatypes.serialization;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import javax.xml.stream.XMLStreamException;

/**
 * Serializer that handles SQL types that are processed as {@link Reader}  
 * @author Uli Schneider
 */
public class ReaderSerializer implements XMLSerializer<Reader> {
	
	private static final int BUFFER_SIZE = 100000;
	
	@Override
	public void serialize(Reader reader, XMLContentCrafter consumer)	throws XMLStreamException, IOException {
		char[] buff = new char[BUFFER_SIZE];
		int i=0;
		while((i=reader.read(buff))!=-1) {
			final String string = new String(buff,0,i);
			byte[] stringBytes = string.getBytes(Charset.forName("UTF-16"));
			consumer.craft(SerializationUtils.bytesToHex(stringBytes, stringBytes.length));
		}
	}
	
	@Override
	public XMLContentConverter<Reader> newDeserializer() {
		return new ReaderConverter();
		
	}

	private static final class ReaderConverter implements XMLContentConverter<Reader> {
		private Reader reader = null;
		private OutputStream os;
		private Character truncatedChar=null;
	
		@Override
		public Reader process(String xmlContent) throws IOException {
			if(this.os==null) {
				final Path clobFile = Files.createTempFile(null,null);
				this.reader=Files.newBufferedReader(clobFile,Charset.forName("UTF-16"));
				this.os=Files.newOutputStream(clobFile, StandardOpenOption.DELETE_ON_CLOSE,StandardOpenOption.WRITE);
			}
			if(this.truncatedChar!=null) {
				xmlContent=this.truncatedChar+xmlContent;
				this.truncatedChar=null;
			}
			final int length = xmlContent.length();
			if(length%2!=0) {
				this.truncatedChar=xmlContent.charAt(length-1);
			}
			this.os.write(SerializationUtils.hexToBytes(xmlContent));
			return this.reader;
		}
		
		@Override
		public void deserializationComplete() throws IOException {
			if(this.os!=null) {
				this.os.close();
			}
		}
		
		@Override
		public void close() throws IOException {
			if(this.reader!=null) {
				this.reader.close();
			}
		}
	}

}
