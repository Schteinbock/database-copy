package de.us.dbcopy;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import de.us.dbcopy.exception.MissingPropertyException;
import de.us.dbcopy.exception.XMLSchemaMismatchException;
import de.us.dbcopy.database.DatabaseSchemaDefinition;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.exception.DatabaseCopyException;
import de.us.dbcopy.usecase.TableExport;
import de.us.dbcopy.usecase.TableImport;
import de.us.dbcopy.usecase.TableTransfer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command
public class DatabaseCopyCommand {
	
	@Option(names = {"--configuration","-c"},required = true)
	private Path mainConnectionProperties;
	
	public static void main(String[] args) {
		new CommandLine(new DatabaseCopyCommand()).execute(args);
	}
	
	@Command(name = "transfer")
	public void transfer() throws IOException, SQLException, XMLSchemaMismatchException, DatabaseCopyException {
		final CommandProcess<TableTransfer> commandProcess = craftProcess();
		final Map<String, TableDefinition> tableDefinitions = new HashMap<String, TableDefinition>();
		final Connection targetConnection = commandProcess.getConnection("transfer.target");
		final DatabaseSchemaDefinition targetDefinition = new DatabaseSchemaDefinition(targetConnection,commandProcess.getApplicationConfiguration());
		targetDefinition.getTableDefinitions().forEach(e->tableDefinitions.put(e.tableName(), e));
		commandProcess.setPreparer(t -> new TableTransfer(t, tableDefinitions.get(t.tableName()),commandProcess.getApplicationConfiguration()));
		commandProcess.setProcessor(e->e.copyTable(commandProcess.getSourceConnection(), targetConnection));
		commandProcess.process();
	}
	
	@Command(name = "export")
	public void exportDB() throws IOException, SQLException, XMLSchemaMismatchException, DatabaseCopyException {
		final CommandProcess<TableExport> tableExport = craftProcess();
		tableExport.setPreparer(e->new TableExport(e,tableExport.getApplicationConfiguration()));
		tableExport.setProcessor(e->e.exportTable(tableExport.getSourceConnection()));
		tableExport.process();
	}

	@Command(name = "import")
	public void importDB() throws IOException, SQLException, XMLSchemaMismatchException, DatabaseCopyException {
		final CommandProcess<TableImport> tableExport = craftProcess();
		tableExport.setPreparer(e->new TableImport(e,tableExport.getApplicationConfiguration()));
		tableExport.setProcessor(e->e.importTable(tableExport.getSourceConnection()));
		tableExport.process();
	}

	private <T> CommandProcess<T> craftProcess() throws MissingPropertyException, IOException, SQLException {
		return new CommandProcess<T>(this.mainConnectionProperties);
	}
}
