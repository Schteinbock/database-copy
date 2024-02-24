package de.us.dbcopy.datatypes;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import javax.xml.stream.XMLStreamException;

import de.us.dbcopy.datatypes.serialization.XMLContentCrafter;
import de.us.dbcopy.datatypes.serialization.DeserializationValue;
import de.us.dbcopy.datatypes.serialization.XMLContentConverter;
import de.us.dbcopy.datatypes.serialization.XMLSerializer;
import de.us.dbcopy.exception.DatabaseCopyRuntimeException;


class ValueMapper<T> {

	private final Function<ResultSet,GetOfResultSet<T>> resultSetFunction;
	private final Function<PreparedStatement,SetToStatement<T>> prepareStatementFunction;
	private final XMLSerializer<T> objectSerializer;
	

	ValueMapper(final Function<ResultSet,GetOfResultSet<T>> resultSetFunction
			,final Function<PreparedStatement,SetToStatement<T>> prepareStatementFunction
			,final XMLSerializer<T> serializer) {
		this.resultSetFunction = resultSetFunction;
		this.prepareStatementFunction = prepareStatementFunction;
		this.objectSerializer=serializer;
	}
	
	void setValue(ResultSet resultSet, PreparedStatement statement,int colIndex) throws SQLException {
		setValue(statement, colIndex, getValue(resultSet, colIndex));
	}
	
	void setValue(PreparedStatement statement,int colIndex,T value) throws SQLException {
		this.prepareStatementFunction.apply(statement).set(colIndex, value);
	}
	
	T getValue(ResultSet resultSet,int colIndex) throws SQLException {
		return this.resultSetFunction.apply(resultSet).get(colIndex);
	}
	
	void serializeObject(ResultSet resultSet,int colIndex,XMLContentCrafter crafter) throws SQLException, XMLStreamException, IOException {
		T value = getValue(resultSet, colIndex);
		if(value==null) {
			crafter.craft(null);
		} else {
			this.objectSerializer.serialize(value, crafter);
		}
	}
	
	/**
	 * Returns an {@link XMLContentProcessor} which internally handles the {@link PreparedStatement}.
	 * Values written to the {@link XMLContentProcessor} are passed to the {@link XMLContentConverter} which
	 * assures that value integrity is sustained.
	 */
	XMLContentProcessor newContentProcessor(final PreparedStatement ps,final int colIndex) {
		return new XMLContentProcessor() {

			private T value=null;
			
			private XMLContentConverter<T> contentConverter;
			
			@Override
			public void process(String xmlContent) throws IOException {
				final DeserializationValue<T> deserializationValue;
				if(this.contentConverter == null) {
					this.contentConverter= (xmlContent == null ? e->null : ValueMapper.this.objectSerializer.newDeserializer());
					deserializationValue=this.contentConverter.getDeserializationValue(xmlContent);
				} else {
					 deserializationValue = this.contentConverter.getDeserializationValue(xmlContent);
					 if(!deserializationValue.isNewValue() && deserializationValue.getValue()!=this.value) {
						 throw new DatabaseCopyRuntimeException("Values must match!");
					 }
				}
				this.value = deserializationValue.getValue();
			}
			
			@Override
			public void deserializationComplete() throws SQLException, IOException {
				this.contentConverter.deserializationComplete();
				SetToStatement<T> setToStatement = ValueMapper.this.prepareStatementFunction.apply(ps);
				setToStatement.set(colIndex, this.value);
			}
			
			
			@Override
			public void close() throws IOException {
				this.contentConverter.close();
			}
			
		};
		
	}
	
	

}
