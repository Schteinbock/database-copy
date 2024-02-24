package de.us.dbcopy;

import java.io.IOException;
import java.sql.SQLException;

import de.us.dbcopy.exception.DatabaseCopyException;
import de.us.dbcopy.exception.XMLSchemaMismatchException;

interface CommandTableStepProcessor<T> {
	
	public void executeCommand(T command) throws XMLSchemaMismatchException,IOException,DatabaseCopyException,SQLException;
}
