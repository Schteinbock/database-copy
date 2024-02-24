package de.us.dbcopy.tabletransfer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import de.us.dbcopy.database.DatabaseSchemaDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.exception.DatabaseCopyException;
import de.us.dbcopy.usecase.TableTransfer;

public class TableTransferTest {
	
	@Test
	public void test_DatabaseCopy() throws SQLException, DatabaseCopyException, IOException {
		Connection sourceConnection = DriverManager.getConnection("jdbc:h2:mem:");
		Connection targetConnection = DriverManager.getConnection("jdbc:h2:mem:");
		Statement statementCon1 = sourceConnection.createStatement();
		Statement statementCon2 = targetConnection.createStatement();
		statementCon2.execute("CREATE TABLE TEST1 (TESTCOL1 VARCHAR(100))");
		statementCon2.close();
		statementCon1.execute("CREATE TABLE TEST1 (TESTCOL1 VARCHAR(100))");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST1')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST2')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST3')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST4')");
		statementCon1.execute("INSERT INTO TEST1 VALUES ('TEST5')");
		statementCon1.close();
		DatabaseSchemaDefinition sourceSchemaDefinition = new DatabaseSchemaDefinition(sourceConnection,Collections.emptyMap());
		DatabaseSchemaDefinition targetSchemaDefinition = new DatabaseSchemaDefinition(targetConnection,Collections.emptyMap());
		TreeSet<TableDefinition> sourceTables = sourceSchemaDefinition.getTableDefinitions();
		TreeSet<TableDefinition> targetTables = targetSchemaDefinition.getTableDefinitions();
		Assert.assertEquals("Tables must match",1,sourceTables.size());
		Assert.assertEquals("Tables must match",1,targetTables.size());
		TableDefinition sourceTable = sourceTables.iterator().next();
		TableDefinition targetTable = targetTables.iterator().next();
		TableTransfer tableTransfer = new TableTransfer(sourceTable, targetTable,Collections.singletonMap("transfer.commitcount","1000"));
		tableTransfer.copyTable(sourceConnection, targetConnection);
		
	}
}
