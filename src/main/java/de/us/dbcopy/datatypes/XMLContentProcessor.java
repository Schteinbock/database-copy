package de.us.dbcopy.datatypes;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import de.us.dbcopy.datatypes.serialization.XMLSerializer;

/**
 * Processor which internally handles the conversion of String values to SQL values.
 */
public interface XMLContentProcessor extends AutoCloseable {
	
	/**
	 * Write the given string to an SQL {@link PreparedStatement}. Internally
	 * this {@link String} is processed to a {@link XMLSerializer}.
	 * 
	 * @param content
	 * @throws SQLException If the JDBC driver throws such exception
	 * @throws IOException If an {@link IOException} occures during conversion
	 * for example if connected to a stream or reader.
	 */
	public void process(String content) throws IOException;

	/**
	 * Indicates that no further call to {@link #process(String)} will be made and
	 * the deserialized value can be written to the database.
	 * @throws SQLException 
	 * @throws IOException
	 */
	public void deserializationComplete() throws SQLException, IOException;
	
	/**
	 * This method may be called after the
	 * Releases any resources (Streams, Readers) after a value
	 * has been completely written to the database. 
	 * @throws IOException
	 */	
	@Override
	public void close() throws IOException;
		

}
