package de.us.dbcopy.exportschema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.junit.Test;

import de.us.dbcopy.database.DatabaseSchemaDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.exception.DatabaseCopyException;
import de.us.dbcopy.usecase.TableExport;

public class TableExportTest {
	
	@Test
	public void test_DatabaseCopy() throws SQLException, DatabaseCopyException, IOException {
		Connection sourceConnection = DriverManager.getConnection("jdbc:h2:mem:");
		Statement statementCon1 = sourceConnection.createStatement();
		statementCon1.execute("CREATE TABLE TEST1 (TESTCOL1 VARCHAR(100))");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST1')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST2')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST3')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST4')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST5')");
		statementCon1.close();
		DatabaseSchemaDefinition sourceSchemaDefinition = new DatabaseSchemaDefinition(sourceConnection,Collections.emptyMap());
		for (TableDefinition definition : sourceSchemaDefinition.getTableDefinitions()) {
			Path tempFile = Files.createTempDirectory("exporttestdir");
			TableExport tableExport = new TableExport(definition,Collections.singletonMap("export.targetdir", tempFile.toString()));
			tableExport.exportTable(sourceConnection);
		}
		
	}
}
