package de.us.dbcopy.datatypes;

import java.sql.SQLException;

/**
 * Functional interface 
 * @param <T>
 * @author Uli Schneider
 */
interface GetOfResultSet<T> {
	public T get(int colIndex) throws SQLException;
}
