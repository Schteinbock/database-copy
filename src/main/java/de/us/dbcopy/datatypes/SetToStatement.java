package de.us.dbcopy.datatypes;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Functional Interface used to consume {@link PreparedStatement}s
 * @author Uli Schneider
 */
@FunctionalInterface
interface SetToStatement<T> {
	public abstract void set(int colIndex,T value) throws SQLException;
}
