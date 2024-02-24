package de.us.dbcopy.usecase;

import static de.us.dbcopy.usecase.XMLConstants.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.us.dbcopy.database.ColumnDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.datatypes.JavaDataType;
import de.us.dbcopy.datatypes.XMLContentProcessor;
import de.us.dbcopy.exception.DatabaseCopyRuntimeException;
import de.us.dbcopy.exception.DatabaseSchemaMissmatchException;
import de.us.dbcopy.exception.XMLSchemaMismatchException;

/**
 * @author Uli Schneider
 */
public class TableImport {
	private TableDefinition table;
	private XMLEventReader xmlEventReader;
	private Logger logger = LoggerFactory.getLogger(getClass());
	private Map<String, String> applicationConfiguration;

	
	public TableImport(TableDefinition targetTable,Map<String, String> applicationConfiguration) {
		this.table=targetTable;
		this.applicationConfiguration = applicationConfiguration;
	}

	public void importTable(Connection connection) throws XMLSchemaMismatchException, DatabaseSchemaMissmatchException {
		Path targetFile=Paths.get(this.applicationConfiguration.get("import.sourcedir"));
		final String tableName = this.table.tableName();
		targetFile=targetFile.resolve(String.format("%s.xml.gz",tableName));
		this.logger.info(String.format("Importing table %s from file %s",tableName,targetFile.toString()));
		try(Reader fos = new InputStreamReader(new GZIPInputStream(Files.newInputStream(targetFile, StandardOpenOption.READ)),"UTF-16")) {
			XMLInputFactory xmlOutputFactory = XMLInputFactory.newFactory();
			this.xmlEventReader = xmlOutputFactory.createXMLEventReader(fos);
			XMLEvent xmlEvent = this.xmlEventReader.nextEvent();
			if(!xmlEvent.isStartDocument()) {
				throw new XMLSchemaMismatchException("Start document tag not found in file");
			}
			StartElement tableDefStart = nextAsStart(TABLE_DEF);
			if(!tableDefStart.getAttributeByName(TABLE_DEF_NAME).getValue().equals(this.table.tableName())) {
				throw new XMLSchemaMismatchException("Table definition tag does not contain table name!");
			}
			this.logger.info(String.format("Checking that table definition of %s matches",tableName));
			checkTableDefinition();
			importData(connection);
			nextAsEnd(TABLE_DEF);
			xmlEvent = this.xmlEventReader.nextEvent();
			if(!xmlEvent.isEndDocument()) {
				throw new XMLSchemaMismatchException("End document tag not found in file");
			}
			this.xmlEventReader.close();
		} catch (XMLStreamException | IOException | SQLException e) {
			throw new DatabaseCopyRuntimeException(e);
		}
	}
	
	private void checkTableDefinition() throws XMLStreamException, XMLSchemaMismatchException, DatabaseSchemaMissmatchException {
		nextAsStart(TABLE_DEF_COLUMNS);
		Iterator<ColumnDefinition> columnIterator = table.columns().iterator();
		XMLEvent event;
		while(!(event=this.xmlEventReader.nextEvent()).isEndElement()) {
			ColumnDefinition columnDefinition = columnIterator.next();
			StartElement startElement = event.asStartElement();
			Attribute columnNameAttribute = startElement.getAttributeByName(TABLE_DEF_COLUMN_NAME);
			String columnName = columnNameAttribute.getValue();
			if(!columnName.equals(columnDefinition.columnName())) {
				throw new DatabaseSchemaMissmatchException();
			}
			JavaDataType dataTypeInXml = JavaDataType.valueOf(nextAsCharacters().getData());
			JavaDataType dataTypeInTargetColDef = columnDefinition.dataTypes().iterator().next();
			if(dataTypeInXml!=dataTypeInTargetColDef) {
				throw new DatabaseSchemaMissmatchException();
			}
			nextAsEnd(TABLE_DEF_COLUMN);
		}
		if(!TABLE_DEF_COLUMNS.equals(event.asEndElement().getName())) {
			throw new XMLSchemaMismatchException("Table Columns tag not properly closed!");
		}
	}
	
	private void importData(Connection connection) throws XMLStreamException, SQLException, IOException, XMLSchemaMismatchException {
		this.logger.info(String.format("Starting import to table %s",this.table.tableName()));
		StringBuilder insertStatementSQL = new StringBuilder("INSERT INTO ");
		insertStatementSQL.append(this.table.tableName()).append(" (");
		TreeSet<ColumnDefinition> columns = this.table.columns();
		for (ColumnDefinition columnDef : columns) {
			insertStatementSQL.append(columnDef.columnName()).append(',');
		}
		insertStatementSQL.deleteCharAt(insertStatementSQL.length()-1).append(')');
		insertStatementSQL.append(" VALUES (");
		StringBuilder placeholders = new StringBuilder(new String(new char[this.table.columns().size()]).replace("\0", "?,"));
		insertStatementSQL.append(placeholders).deleteCharAt(insertStatementSQL.length()-1).append(')');
		String commitCountString = this.applicationConfiguration.getOrDefault("base.commitcount","1000000");
		final int commitcount = Integer.valueOf(commitCountString);
		this.logger.debug(String.format("Insert statement: \"%s\"",insertStatementSQL));
		PreparedStatement insertStatement = connection.prepareStatement(insertStatementSQL.toString());
		nextAsStart(TABLE_DATA_DEF);
		XMLEvent event;
		long rows=0;
		final Queue<XMLContentProcessor> processors = new ArrayDeque<>(commitcount);
		while(!(event=this.xmlEventReader.nextEvent()).isEndElement()) {
			if(!event.isStartElement() || !event.asStartElement().getName().equals(ROW_DATA_DEF)) {
				throw new XMLSchemaMismatchException();
			}
			rows++;
			if((rows%commitcount)==0) {
				this.logger.info(String.format("Imported %d rows into table %s. Still on it.",rows,this.table.tableName()));
				processBatch(insertStatement,processors);
			}
			
			final Iterator<ColumnDefinition> tableColumns = this.table.columns().iterator();
			int i=0;
			while(!(event=this.xmlEventReader.nextEvent()).isEndElement()) {
				if(!event.isStartElement() || !event.asStartElement().getName().equals(COLUMN_VALUE_DEF)) {
					throw new XMLSchemaMismatchException();
				}
				ColumnDefinition colDef = tableColumns.next();
				i++;
				
				final XMLContentProcessor dataType=colDef.dataTypes().iterator().next().getNewContentProcessor(insertStatement, i);
				processors.add(dataType);
				final Attribute nullValue = event.asStartElement().getAttributeByName(COLUMN_NULL_VALUE_DEF);
				if(nullValue!=null && nullValue.getValue().equals("true")) {
					dataType.process(null);
					event=this.xmlEventReader.nextEvent();
				} else {
					boolean emptyString=true;
					while((event=this.xmlEventReader.nextEvent()).isCharacters()) {
						emptyString=false;
						String content = event.asCharacters().getData();
						dataType.process(content);
					}
					if(emptyString) {
						dataType.process("");
					}
				}
				dataType.deserializationComplete();
				if(!event.isEndElement() || !event.asEndElement().getName().equals(COLUMN_VALUE_DEF)) {
					throw new XMLSchemaMismatchException();
				}
			}
			insertStatement.addBatch();
		}
		if((rows%commitcount)>0) {
			processBatch(insertStatement,processors);
		}
		insertStatement.close();
		if(!TABLE_DATA_DEF.equals(event.asEndElement().getName())) {
			throw new XMLSchemaMismatchException();
		}
		this.logger.info(String.format("Finished import of table %s",this.table.tableName()));
	}
	
	private void processBatch(PreparedStatement insertStatement,Queue<XMLContentProcessor> processors) throws SQLException,IOException {
		insertStatement.executeBatch();
		insertStatement.clearBatch();
		while(!processors.isEmpty()) {
			processors.poll().close();
		}
	}
	
	private StartElement nextAsStart(QName expectedEvent) throws XMLSchemaMismatchException, XMLStreamException {
		XMLEvent nextEvent = this.xmlEventReader.nextEvent();
		StartElement event = nextEvent.asStartElement();
		if(!expectedEvent.equals(event.getName())) {
			final String errorMessage = String.format("Expected opening %s tag but got %s",expectedEvent.toString(),event.toString());
			throw new XMLSchemaMismatchException(errorMessage);
		}
		return event;
	}

	private EndElement nextAsEnd(QName expectedEvent) throws XMLSchemaMismatchException, XMLStreamException {
		XMLEvent nextEvent = this.xmlEventReader.nextEvent();
		EndElement event = nextEvent.asEndElement();
		if(!expectedEvent.equals(event.getName())) {
			final String errorMessage = String.format("Expected closing %s tag but got %s",expectedEvent.toString(),event.toString());
			throw new XMLSchemaMismatchException(errorMessage);
		}
		return event;
	}
	
	private Characters nextAsCharacters() throws XMLStreamException {
		return this.xmlEventReader.nextEvent().asCharacters();
	}
}
