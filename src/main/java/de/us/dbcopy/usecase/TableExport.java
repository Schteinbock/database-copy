package de.us.dbcopy.usecase;

import static de.us.dbcopy.usecase.XMLConstants.*;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndDocument;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartDocument;
import javax.xml.stream.events.StartElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.us.dbcopy.database.ColumnDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.datatypes.JavaDataType;
import de.us.dbcopy.exception.DatabaseCopyRuntimeException;

/**
 * @author Uli Schneider
 */
public class TableExport {
	private TableDefinition table;

	private XMLEventWriter xmlEventWriter;

	private XMLEventFactory xmlEventFactory;
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private Map<String, String> applicationConfiguration;

	public TableExport(TableDefinition tableDefinition,Map<String,String> applicationConfiguration) {
		this.table = tableDefinition;
		this.applicationConfiguration = applicationConfiguration;
	}
	
	public void exportTable(Connection connection) throws IOException, SQLException {
		Path targetDirectory = Paths.get(this.applicationConfiguration.get("export.targetdir"));
		Path targetFile = targetDirectory.resolve(this.table.tableName()+".xml.gz");
		this.logger.info(String.format("Now exporting table %s to file %s",this.table.tableName(),targetFile));
		try(Writer fos = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(targetFile)), "UTF-16")) {
			XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newFactory();
			this.xmlEventWriter = xmlOutputFactory.createXMLEventWriter(fos);
			this.xmlEventFactory = XMLEventFactory.newFactory();
			StartDocument startDocument = this.xmlEventFactory.createStartDocument();
			this.xmlEventWriter.add(startDocument);
			createStartElement(TABLE_DEF);
			Attribute tableNameAttribute = this.xmlEventFactory.createAttribute(TABLE_DEF_NAME,this.table.tableName());
			this.xmlEventWriter.add(tableNameAttribute);
			this.logger.debug("Creating table information header.");
			createTableDefinition();
			this.logger.debug("Successfully created table information header.");
			dumpData(connection);
			createEndElement(TABLE_DEF);
			EndDocument endDocument = xmlEventFactory.createEndDocument();
			this.xmlEventWriter.add(endDocument);
			this.xmlEventWriter.close();
		} catch (XMLStreamException e) {
			throw new DatabaseCopyRuntimeException(e);
		}
	}
	
	private void dumpData(Connection connection) throws SQLException, XMLStreamException, IOException {
		this.logger.info(String.format("Dumping data of table %s",this.table.tableName()));
		createStartElement(TABLE_DATA_DEF);
		final StringBuilder selectString =new StringBuilder("SELECT ");
		final Statement statement = connection.createStatement();
		final JavaDataType[] columns = new JavaDataType[this.table.columns().size()];
		int i=0;
		for (ColumnDefinition columnDefinition : this.table.columns()) {
			selectString.append(columnDefinition.columnName()).append(',');
			columns[i]=columnDefinition.dataTypes().iterator().next();
			i++;
		}
		final String commitCountString = this.applicationConfiguration.getOrDefault("base.commitcount","1000000");
		final int commitcount = Integer.valueOf(commitCountString);
		selectString.deleteCharAt(selectString.length()-1);
		selectString.append(" FROM ");
		selectString.append(this.table.tableName());
		this.logger.debug(String.format("Select Statement: \"%s\"",selectString.toString()));
		statement.execute(selectString.toString());
		ResultSet resultSet = statement.getResultSet();
		long rows=0;
		while(resultSet.next()) {
			createStartElement(ROW_DATA_DEF);
			rows++;
			if((rows%commitcount)==0) {
				this.logger.info(String.format("Dumped %d rows of table %s. Still on it.",rows,this.table.tableName()));
			}
			for (i = 0; i < columns.length;) {
				final JavaDataType columnDataType = columns[i];
				createStartElement(COLUMN_VALUE_DEF);
				columnDataType.serializeValue(resultSet, ++i,content -> {
					if(content==null) {
						Attribute nullAttribute = TableExport.this.xmlEventFactory.createAttribute(COLUMN_NULL_VALUE_DEF, "true");
						TableExport.this.xmlEventWriter.add(nullAttribute);
					} else {
						createCharacter(content);
					}
				});
				createEndElement(COLUMN_VALUE_DEF);
			}
			createEndElement(ROW_DATA_DEF);
		}
		createEndElement(TABLE_DATA_DEF);
		this.logger.info(String.format("Dumping data of table %s completed",this.table.tableName()));
	}
	
	private void createTableDefinition() throws XMLStreamException {
		createStartElement(TABLE_DEF_COLUMNS);
		for (ColumnDefinition columnDefinition : this.table.columns()) {
			createStartElement(TABLE_DEF_COLUMN);
			this.xmlEventWriter.add(this.xmlEventFactory.createAttribute(TABLE_DEF_COLUMN_NAME, columnDefinition.columnName()));
			createCharacter(columnDefinition.dataTypes().iterator().next().toString());
			createEndElement(TABLE_DEF_COLUMN);
		}
		createEndElement(TABLE_DEF_COLUMNS);
	}
	
	private StartElement createStartElement(QName name) throws XMLStreamException {
		StartElement startElement = this.xmlEventFactory.createStartElement(name, null, null);
		this.xmlEventWriter.add(startElement);
		return startElement;
	}
	
	private EndElement createEndElement(QName name) throws XMLStreamException {
		EndElement startElement = this.xmlEventFactory.createEndElement(name, null);
		this.xmlEventWriter.add(startElement);
		return startElement;
	}
	
	private Characters createCharacter(String chars) throws XMLStreamException {
		final Characters charEvent = this.xmlEventFactory.createCharacters(chars);
		this.xmlEventWriter.add(charEvent);
		return charEvent;
	}
}
