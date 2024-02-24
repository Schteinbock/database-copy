package de.us.dbcopy.database;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLFileProcessor {
	
	private Connection connection;
	
	private static final Pattern END_OF_STATEMENT = Pattern.compile(".*;$");
	
	private final Logger logger = LoggerFactory.getLogger(getClass());

	public SQLFileProcessor(Connection connection) {
		this.connection = connection;
	}
	
	public void processFile(Path fileToRead) throws SQLException,IOException {
		StringBuilder statementBuilder = new StringBuilder();
		for (String line : Files.readAllLines(fileToRead, StandardCharsets.UTF_8)) {
			Matcher m = END_OF_STATEMENT.matcher(line);
			statementBuilder.append(line);
			if(m.find()) {
				statementBuilder.deleteCharAt(statementBuilder.length()-1);
				process(statementBuilder.toString());
			}
		}
	}
	
	private void process(final String statement) throws SQLException {
		logger.info(String.format("Processing statement %s",statement));
		try(Statement statementToExecute = this.connection.createStatement()) {;
			statementToExecute.execute(statement);
			statementToExecute.close();
		}
	}
	

}
