package de.us.dbcopy.datatypes;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import javax.xml.stream.XMLStreamException;

import de.us.dbcopy.datatypes.serialization.BufferedSerializer;
import de.us.dbcopy.datatypes.serialization.ReaderSerializer;
import de.us.dbcopy.datatypes.serialization.StreamSerializer;
import de.us.dbcopy.datatypes.serialization.StringSerializer;
import de.us.dbcopy.datatypes.serialization.StringToHexSerializer;
import de.us.dbcopy.datatypes.serialization.XMLContentCrafter;
import de.us.dbcopy.usecase.TableTransfer;

/**
 * Java Data Types JDBC supports. Each {@link JavaDataType} has a {@link ValueMapper} that handles
 * value conversion.
 */
public enum JavaDataType {
	/**
	 * @see ResultSet#getString(int)
	 * @see PreparedStatement#setString(int, String)
	 * @see StringToHexSerializer
	 */
	STRING(new ValueMapper<String>(rs->rs::getString,ps->ps::setString,new StringToHexSerializer<>(String::valueOf,String::valueOf))),
	/**
	 * @see ResultSet#getInt(int)
	 * @see PreparedStatement#setInt(int, int)
	 * @see StringSerializer
	 */
	INTEGER(new ValueMapper<>(rs->rs::getInt,ps->ps::setInt,new StringSerializer<>(Integer::valueOf))),
	/**
	 * @see ResultSet#getShort(int)
	 * @see PreparedStatement#setShort(int, short)
	 * @see StringSerializer
	 */
	SHORT(new ValueMapper<>(rs->rs::getShort,ps->ps::setShort,new StringSerializer<>(Short::valueOf))),
	/**
	 * @see ResultSet#getBoolean(int)
	 * @see PreparedStatement#setBoolean(int, boolean)
	 * @see StringSerializer
	 */
	BOOLEAN(new ValueMapper<>(rs->rs::getBoolean,ps->ps::setBoolean,new StringSerializer<>(Boolean::valueOf))),
	/**
	 * @see ResultSet#getDouble(int)
	 * @see PreparedStatement#setDouble(int, double)
	 * @see BufferedSerializer
	 */
	DOUBLE(new ValueMapper<>(rs->rs::getDouble,ps->ps::setDouble,new BufferedSerializer<>(8,(bb,v)->bb.putDouble(v),(bb)->bb.getDouble()))),
	/**
	 * @see ResultSet#getLong(int)
	 * @see PreparedStatement#setLong(int, long)
	 * @see StringSerializer
	 */
	LONG(new ValueMapper<>(rs->rs::getLong,ps->ps::setLong,new StringSerializer<>(Long::valueOf))),
	/**
	 * @see ResultSet#getBigDecimal(int)
	 * @see PreparedStatement#setBigDecimal(int, BigDecimal)
	 * @see StringToHexSerializer
	 */
	BIGDECIMAL(new ValueMapper<>(rs->rs::getBigDecimal,ps->ps::setBigDecimal,new StringToHexSerializer<>(e->e.toPlainString(),BigDecimal::new))),
	/**
	 * @see ResultSet#getFloat(int)
	 * @see PreparedStatement#setFloat(int, float)
	 * @see BufferedSerializer
	 */
	FLOAT(new ValueMapper<>(rs->rs::getFloat,ps->ps::setFloat,new BufferedSerializer<>(4,(bb,v)->bb.putFloat(v),(bb)->bb.getFloat()))),
	/**
	 * @see ResultSet#getTime(int)
	 * @see PreparedStatement#setTime(int, Time)
	 * @see StringSerializer
	 */
	TIME(new ValueMapper<>(rs->rs::getTime,ps->ps::setTime,new StringSerializer<>(Time::valueOf))),
	/**
	 * @see ResultSet#getTimestamp(int)
	 * @see PreparedStatement#setTimestamp(int, Timestamp)
	 * @see StringSerializer
	 */
	TIMESTAMP(new ValueMapper<>(rs->rs::getTimestamp,ps->ps::setTimestamp,new StringSerializer<>(Timestamp::valueOf))),
	/**
	 * @see ResultSet#getDate(int)
	 * @see PreparedStatement#setDate(int, Date)
	 * @see StringSerializer
	 */
	DATE(new ValueMapper<>(rs->rs::getDate,ps->ps::setDate,new StringSerializer<>(Date::valueOf))),
	/**
	 * @see ResultSet#getBinaryStream(int)
	 * @see PreparedStatement#setBinaryStream(int, java.io.InputStream)
	 * @see StreamSerializer
	 */
	BINARYSTREAM(new ValueMapper<>(rs->rs::getBinaryStream,ps->ps::setBinaryStream,new StreamSerializer())),
	/**
	 * @see ResultSet#getBlob(int)
	 * @see PreparedStatement#setBlob(int, java.io.InputStream)
	 * @see StreamSerializer
	 */
	BLOB(new ValueMapper<>(rs->(get(rs::getBlob,(s->s.getBinaryStream()))),ps->ps::setBlob,new StreamSerializer())),
	/**
	 * @see ResultSet#getNClob(int)
	 * @see PreparedStatement#setNClob(int, java.io.Reader)
	 * @see ReaderSerializer
	 */
	NCLOB(new ValueMapper<>(rs->(get(rs::getNClob,(s->s.getCharacterStream()))),ps->ps::setNClob,new ReaderSerializer())),
	/**
	 * @see ResultSet#getClob(int)
	 * @see PreparedStatement#setClob(int, java.io.Reader)
	 * @see ReaderSerializer
	 */
	CLOB(new ValueMapper<>(rs->(get(rs::getClob,(s->s.getCharacterStream()))),ps->ps::setClob,new ReaderSerializer())),
	/**
	 * @see ResultSet#getAsciiStream(int)
	 * @see PreparedStatement#setAsciiStream(int, java.io.InputStream)
	 * @see StreamSerializer
	 */
	ASCII_STREAM(new ValueMapper<>(rs->rs::getAsciiStream,ps->ps::setAsciiStream,new StreamSerializer())),
	;
	private final ValueMapper<?> valueMapper;
	private <T> JavaDataType(ValueMapper<T> valueMapper) {
		this.valueMapper = valueMapper;
	}
	
	/**
	 * This method is used to connect the value of a {@link ResultSet} to the corresponding {@link PreparedStatement}.
	 * Used by {@link TableTransfer} to connect the values with each other.
	 * 
	 * @throws SQLException If the JDBC throws a technical error. 
	 */
	public void mapValue(ResultSet resultSet,PreparedStatement preparedStatement, int colIndex) throws SQLException {
		this.valueMapper.setValue(resultSet, preparedStatement, colIndex);
	}
	
	/**
	 * Serializes a value reporting it to the {@link XMLContentCrafter}
	 * @throws SQLException If the JDBC throws a technical error reading the value from the {@link ResultSet}.
	 * @throws XMLStreamException If the {@link XMLContentCrafter} throws such exception
	 * @throws IOException If the {@link XMLContentCrafter} throws such exception while processing.
	 */
	public void serializeValue(ResultSet resultSet,int colIndex,XMLContentCrafter serializer) throws SQLException, XMLStreamException, IOException {
		this.valueMapper.serializeObject(resultSet, colIndex, serializer);
	}
	
	/**
	 * Provides a {@link XMLContentProcessor} that wraps around the {@link PreparedStatement}
	 */
	public XMLContentProcessor getNewContentProcessor(PreparedStatement ps,int colIndex) {
		return this.valueMapper.newContentProcessor(ps, colIndex);
	}
	
	private static <T,K> GetOfResultSet<T> get(final GetOfResultSet<K> receiver,final ValueTransformer<T,K> transform) {
		return colIndex -> {
			final K get = receiver.get(colIndex);
			return (get==null?null:transform.transform(get));
		};
	}
	
	private static interface ValueTransformer<T,K> {
		public T transform(K value) throws SQLException;
	}
	
}
