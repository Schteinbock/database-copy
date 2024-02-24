package de.us.dbcopy.database;

import java.util.Objects;
import java.util.TreeSet;

public final class TableDefinition implements Comparable<TableDefinition> {
	private String tableName;
	private TreeSet<ColumnDefinition> columns;

	public TableDefinition(String tableName,TreeSet<ColumnDefinition> columns) {
		this.tableName = tableName;
		this.columns = columns;
	}
	
	public String tableName() {
		return this.tableName;
	}
	
	public TreeSet<ColumnDefinition> columns() {
		return this.columns;
	}
	@Override
	public int hashCode() {
		return Objects.hash(columns(), tableName());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableDefinition other = (TableDefinition) obj;
		return Objects.equals(columns(), other.columns()) && Objects.equals(tableName(), other.tableName());
	}

	@Override
	public int compareTo(TableDefinition o) {
		return this.tableName().compareTo(o.tableName());
	}

}
