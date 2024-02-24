package de.us.dbcopy.importschema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.junit.Ignore;
import org.junit.Test;

import de.us.dbcopy.database.DatabaseSchemaDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.exception.DatabaseCopyException;
import de.us.dbcopy.usecase.TableImport;

public class TableImportTest {
	
	@Test
	@Ignore
	public void test_DatabaseCopy() throws SQLException, DatabaseCopyException, IOException {
		Connection sourceConnection = DriverManager.getConnection("jdbc:h2:mem:");
		Statement statementCon1 = sourceConnection.createStatement();
		statementCon1.execute("CREATE TABLE TEST1 (TESTCOL1 VARCHAR(100))");
		statementCon1.close();
		DatabaseSchemaDefinition sourceSchemaDefinition = new DatabaseSchemaDefinition(sourceConnection,Collections.emptyMap());
		for (TableDefinition definition : sourceSchemaDefinition.getTableDefinitions()) {
			Path tempFile = Files.createTempFile("db", ".xml.gz");
			TableImport tableExport = new TableImport(definition,Collections.singletonMap("import.sourcedir", tempFile.toString()));
			tableExport.importTable(sourceConnection);
		}
		
	}
}
