package de.us.dbcopy.database;

import java.util.Objects;
import java.util.TreeSet;

import de.us.dbcopy.datatypes.JavaDataType;

/**
 * Record Class for the definitions of columns in the database mapped to the {@link JavaDataType}s that are supported.
 * @param  columnName The SQL Name of the column
 * @param dataTypes The Java Data Types that this column is mapped to in JDBC
 * @param tableName The SQL name of the table this column belongs to
 */
public final class ColumnDefinition implements Comparable<ColumnDefinition> {
	private final String columnName;
	private TreeSet<JavaDataType> dataTypes;
	private String tableName;

	public ColumnDefinition(String columnName,TreeSet<JavaDataType> dataTypes,String tableName) {
		this.columnName = columnName;
		this.dataTypes = dataTypes;
		this.tableName = tableName;
	}
	
	public String tableName() {
		return this.tableName;
	}
	
	public TreeSet<JavaDataType> dataTypes() {
		return this.dataTypes;
	}
	
	public String columnName() {
		return this.columnName;
	}

	@Override
	public int hashCode() {
		return Objects.hash(columnName(), dataTypes(), tableName());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnDefinition other = (ColumnDefinition) obj;
		return Objects.equals(columnName(), other.columnName()) && Objects.equals(dataTypes(), other.dataTypes())
				&& Objects.equals(tableName(), other.tableName());
	}

	@Override
	public int compareTo(ColumnDefinition o) {
		return this.columnName().compareTo(o.columnName());
	}
	
}
