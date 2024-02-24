package de.us.dbcopy.usecase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.us.dbcopy.database.ColumnDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.datatypes.JavaDataType;
import de.us.dbcopy.exception.DatabaseSchemaMissmatchException;

/**
 * This class performs the single step of a table transfer, which means copying the content
 * of one table to another.
 */
public class TableTransfer {
	
	private static final String INSERT = "INSERT INTO %s (%s) VALUES (%s)";
	
	private static final String SELECT = "SELECT %s FROM %s";


	private int commitCount;
	private Map<Integer,String> colNoToName;
	
	private Map<String,JavaDataType> colNameToDatatype;
	
	private TableDefinition sourceTableDef;

	private TableDefinition targetTableDef;
	
	private String selectStatement;
	
	private String insertStatement;
	
	private Logger logger = LoggerFactory.getLogger(getClass());

	private Map<String, String> applicationConfiguration;
	
	public TableTransfer(TableDefinition sourceTableDef,TableDefinition targetTableDef,Map<String,String> applicationConfiguration) {
		this.sourceTableDef = sourceTableDef;
		this.targetTableDef = targetTableDef;
		this.applicationConfiguration = applicationConfiguration;
	}
	
	private void prepareCopyTable() throws DatabaseSchemaMissmatchException {
		this.colNoToName=new HashMap<>();
		this.colNameToDatatype = new HashMap<>();
		Iterator<ColumnDefinition> sourceColumns = sourceTableDef.columns().iterator();
		Iterator<ColumnDefinition> targetColumns = targetTableDef.columns().iterator();
		StringBuilder columnsJoinedBuilder=new StringBuilder();
		int i=0;
		while(sourceColumns.hasNext() && targetColumns.hasNext()) {
			ColumnDefinition sourceColumnDefinition = sourceColumns.next();
			ColumnDefinition targetColumnDefintion= targetColumns.next();
			String columnName = sourceColumnDefinition.columnName();
			if(!columnName.equals(targetColumnDefintion.columnName())) {
				throw new DatabaseSchemaMissmatchException();
			}
			JavaDataType targetDataType=null;
			for (JavaDataType dataType : sourceColumnDefinition.dataTypes()) {
				if(targetColumnDefintion.dataTypes().contains(dataType)) {
					targetDataType=dataType;
					break;
				}
			}
			if(targetDataType==null) {
				throw new DatabaseSchemaMissmatchException();
			}
			this.colNameToDatatype.put(columnName, targetDataType);
			this.colNoToName.put(++i, columnName);
			columnsJoinedBuilder.append(columnName).append(',');
			
		}
		columnsJoinedBuilder.deleteCharAt(columnsJoinedBuilder.length()-1);
		StringBuilder placeholders = new StringBuilder(new String(new char[this.colNoToName.size()]).replace("\0", "?,"));
		placeholders.deleteCharAt(placeholders.length()-1);
		this.insertStatement = String.format(INSERT, this.targetTableDef.tableName(),columnsJoinedBuilder,placeholders);
		this.selectStatement = String.format(SELECT, columnsJoinedBuilder.toString(),this.sourceTableDef.tableName());
		String commitCountString = this.applicationConfiguration.get("base.commitcount");
		commitCountString=commitCountString==null?"1000000":commitCountString;
		this.commitCount=Integer.valueOf(commitCountString);
		
	}
	
	/**
	 * Copy this table from the provided source connection into the target.
	 *  
	 * @param sC Source connection where data is read
	 * @param tC Target connection where data is written to
	 * @throws SQLException - If JDBC throws a technical {@link SQLException}
	 * @throws DatabaseSchemaMissmatchException - If the table definitions provided
	 *  in {@link #TableTransfer(TableDefinition, TableDefinition, int)} do not match
	 */
	public void copyTable(Connection sC,Connection tC) throws SQLException, DatabaseSchemaMissmatchException {
		prepareCopyTable();
		this.logger.info(String.format("Copy table %s",this.sourceTableDef.tableName()));
		
		try(PreparedStatement iS=tC.prepareStatement(this.insertStatement);Statement sStatement = sC.createStatement()) {
			sStatement.execute(this.selectStatement);
			java.sql.ResultSet allTableRowsSet = sStatement.getResultSet();
			int rows=0;
			while(allTableRowsSet.next()) {
				rows++;
				if(this.commitCount<rows) {
					this.logger.info(String.format("Processing Batch after %d rows...", this.commitCount));
					iS.executeBatch();
					iS.clearBatch();
					this.logger.info("...done");
					rows=0;
				}
				for (Entry<Integer, String> colNoToName : this.colNoToName.entrySet()) {
					JavaDataType datatypeMapper = this.colNameToDatatype.get(colNoToName.getValue());
					datatypeMapper.mapValue(allTableRowsSet, iS, colNoToName.getKey());
				}
				iS.addBatch();
			}
			if(rows !=0) {
				iS.executeBatch();
			}
			sStatement.close();
			iS.close();
		}
	}
	
}
