package de.us.dbcopy.usecase;


import javax.xml.namespace.QName;

public class XMLConstants {
	
	public static final QName TABLE_DEF = new QName("TableDefinition");
	
	public static final QName TABLE_DEF_NAME = new QName("tableName");

	public static final QName TABLE_DEF_COLUMNS = new QName("Columns");
	
	public static final QName TABLE_DEF_COLUMN = new QName("Column");
	
	public static final QName TABLE_DEF_COLUMN_NAME = new QName("columnName");
	
	public static final QName TABLE_DATA_DEF = new QName("TableData");
	
	public static final QName ROW_DATA_DEF = new QName("Rd");
	
	public static final QName COLUMN_VALUE_DEF = new QName("Cv");
	
	public static final QName COLUMN_NULL_VALUE_DEF = new QName("null");

	
}

