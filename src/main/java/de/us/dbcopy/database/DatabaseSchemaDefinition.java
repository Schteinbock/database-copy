package de.us.dbcopy.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.us.dbcopy.datatypes.JavaDataType;
import de.us.dbcopy.exception.MissingPropertyException;

public class DatabaseSchemaDefinition {
	
	private static final String MAPPING_PROPERTIES = "/de/us/dbcopy/datatypemappings/mappings.properties";
	
	private static final String DB_CONFIG_PROPERTIES = "/de/us/dbcopy/databases.properties";
	
	private static final String GET_TABLES_STATEMENT = "%s.gettables.statement";
	
	private static final String GET_TABLES_ARGUMENT = "%s.gettables.tablenamearg";
	
	private static final String GET_COLUMNS_STATEMENT = "%s.getcolumns.statement";
	
	private static final String GET_COLUMNS_DATATYPE_ARGUMENT = "%s.getcolumns.columntypearg";
	
	private static final String GET_COLUMNS_NAME_ARGUMENT = "%s.getcolumns.columnnamearg";
	
	private static final Pattern DATATYPE_PATTERN = Pattern.compile("^[A-Z _]+");
	
	private static final Pattern PRODUCT_PATTERN = Pattern.compile("^[A-Za-z0-9]+");

	private final Map<String,String> applicationConfiguration;

	private String dbProductName;
	
	private Connection sourceConnection;
	
	private TreeSet<TableDefinition> tablesOfDatabase;
	
	public DatabaseSchemaDefinition(Connection connection,Map<String,String> applicationConfiguration) throws MissingPropertyException, SQLException, IOException {
		this.sourceConnection = connection;
		this.applicationConfiguration = new HashMap<String, String>();
		Properties props = new Properties();
		props.load(getClass().getResource(MAPPING_PROPERTIES).openStream());
		props.forEach((k,v)->this.applicationConfiguration.put(k.toString(),v.toString()));
		props = new Properties();
		props.load(getClass().getResource(DB_CONFIG_PROPERTIES).openStream());
		props.forEach((k,v)->this.applicationConfiguration.put(k.toString(),v.toString()));
		this.applicationConfiguration.putAll(applicationConfiguration);
		this.tablesOfDatabase=new TreeSet<TableDefinition>();
		for (String table : getTables(connection)) {
			final TreeSet<ColumnDefinition> columns = getColumns(connection, table);
			this.tablesOfDatabase.add(new TableDefinition(table, columns));
		}
	}
	
	public TreeSet<TableDefinition> getTableDefinitions() {
		return this.tablesOfDatabase;
	}
	
	
	private TreeSet<String> getTables(Connection connection) throws SQLException, MissingPropertyException {
		final String schema = connection.getSchema().trim();
		String getTablesStatement = getProp(GET_TABLES_STATEMENT);
		getTablesStatement = MessageFormat.format(getTablesStatement, schema);
		final String getTablesArg = getProp(GET_TABLES_ARGUMENT);
		Statement statement = connection.createStatement();
		statement.execute(getTablesStatement);
		final ResultSet tablesResultSet = statement.getResultSet();
		GetOfResultSet valueProvider = getValueProvider(tablesResultSet, getTablesArg);
		TreeSet<String> tables = new TreeSet<String>();
		while(tablesResultSet.next()) {
			tables.add(valueProvider.get());
		}
		return tables;
	}
	
	private GetOfResultSet getValueProvider(final ResultSet resultSet,final String arg) {
		GetOfResultSet valueProvider;
		try {
			final Integer colNo = Integer.valueOf(arg);
			valueProvider = ()->resultSet.getString(colNo).trim();
			
		} catch(NumberFormatException e) {
			valueProvider = ()->resultSet.getString(arg).trim();
		}
		return valueProvider;
	}

	
	private interface GetOfResultSet {
		public abstract String get() throws SQLException;
	}
	private String getDbProductName() throws SQLException, MissingPropertyException {
		if(this.dbProductName==null) {
			DatabaseMetaData dbMetaData = this.sourceConnection.getMetaData();
			String productName = dbMetaData.getDatabaseProductName();
			Matcher matcher = PRODUCT_PATTERN.matcher(productName);
			if(!matcher.find()) {
				throw new MissingPropertyException();
			}
			this.dbProductName = matcher.group().toLowerCase();
		}
		return dbProductName;
	}
	
	private String getProp(String property) throws MissingPropertyException, SQLException {
		final String dbProductName = getDbProductName();
		final String propName = String.format(property,dbProductName);
		if(!this.applicationConfiguration.containsKey(propName)) {
			throw new MissingPropertyException(String.format("The following property could not be found: %s",propName));
		}
		return this.applicationConfiguration.get(propName);
	}
	
	private TreeSet<ColumnDefinition> getColumns(Connection connection,String tableName) throws SQLException, MissingPropertyException {
		final TreeSet<ColumnDefinition> columnToDataTypes = new TreeSet<>();
		String schema = connection.getSchema();
		String getColumnsStatement = getProp(GET_COLUMNS_STATEMENT);
		getColumnsStatement=MessageFormat.format(getColumnsStatement, tableName,schema);
		Statement statement = connection.createStatement();
		statement.execute(getColumnsStatement);
		String getColumnName = getProp(GET_COLUMNS_NAME_ARGUMENT);
		String getColumnType = getProp(GET_COLUMNS_DATATYPE_ARGUMENT);
		ResultSet resultSet = statement.getResultSet();
		GetOfResultSet columnTypeProvider = getValueProvider(resultSet, getColumnType);
		GetOfResultSet columnNameProvider = getValueProvider(resultSet, getColumnName);
		while(resultSet.next()) {
			String columnName = columnNameProvider.get();
			String columnType = columnTypeProvider.get();
			Matcher matcher = DATATYPE_PATTERN.matcher(columnType);
			if(!matcher.find()) {
				throw new MissingPropertyException("Datatypepattern not found");
			}
			columnType=matcher.group();
			TreeSet<JavaDataType> dataTypes = new TreeSet<>();
			String mappingEntry;
			try {
				mappingEntry = getMappingProp("%s.datatypes.%s",columnType);
			} catch(MissingPropertyException e) {
				mappingEntry = getMappingProp("%s.datatypes.%s",columnType);
			}
			Arrays.asList(mappingEntry.split(",")).forEach(e->dataTypes.add(JavaDataType.valueOf(e)));
			columnToDataTypes.add(new ColumnDefinition(columnName, dataTypes,tableName));
		}
		return columnToDataTypes;
	}
	
	private String getMappingProp(final String property,final String dbType) throws MissingPropertyException, SQLException {
		String propName = String.format(property,getDbProductName(),dbType);
		if(this.applicationConfiguration.containsKey(propName)) {
			return this.applicationConfiguration.get(propName);
		}
		propName = String.format(property,"sql",dbType);
		if(this.applicationConfiguration.containsKey(propName)) {
			return this.applicationConfiguration.get(propName);
		}
		propName = String.format(property+" or "+property,getDbProductName(),dbType,"sql",dbType);
		throw new MissingPropertyException(String.format("The following property could not be found: (%s)",propName));
	}

}
