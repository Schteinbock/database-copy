package de.us.dbcopy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.us.dbcopy.database.DatabaseSchemaDefinition;
import de.us.dbcopy.database.SQLFileProcessor;
import de.us.dbcopy.database.TableDefinition;
import de.us.dbcopy.exception.DatabaseCopyException;
import de.us.dbcopy.exception.DatabaseCopyRuntimeException;
import de.us.dbcopy.exception.MissingPropertyException;
import de.us.dbcopy.exception.XMLSchemaMismatchException;

public class CommandProcess<T> {
	
	private DatabaseSchemaDefinition schemaDefinition;
	private List<String> includeTables;
	private List<String> excludeTables;
	private CommandTableStepProcessor<T> processor;
	private final Connection sourceConnection;
	private Function<TableDefinition, T> preparer;
	private final Map<String,String> applicationConfiguration;
	
	private static Logger LOGGER = LoggerFactory.getLogger(CommandProcess.class);
	
	public CommandProcess(Path connectionProps) throws MissingPropertyException, IOException, SQLException {
		final Map<String,String> allProps = new HashMap<>();
		BiConsumer<Object,Object> toProps = (k,v)->allProps.put(String.valueOf(k), String.valueOf(v));
		loadProperties(props->props.load(Files.newBufferedReader(connectionProps)), toProps);
		this.applicationConfiguration = Collections.unmodifiableMap(allProps);
		this.sourceConnection = getConnection("base.connection");
		this.schemaDefinition = new DatabaseSchemaDefinition(this.getSourceConnection(),getApplicationConfiguration());
		String exclude = (getApplicationConfiguration().get("base.exclude"));
		this.excludeTables = (exclude==null?Collections.emptyList():Arrays.asList(exclude.split(",")));
		String include = getApplicationConfiguration().get("base.include");
		this.includeTables = (include==null?Collections.emptyList():Arrays.asList(include.split(",")));
	}
	
	private static void loadProperties(LoadProperties propLoader, BiConsumer<Object, Object> addToProps) throws IOException {
		Properties props = new Properties();
		propLoader.loadProperties(props);
		props.forEach(addToProps);
	}
	
	public void process() throws XMLSchemaMismatchException, IOException, DatabaseCopyException, SQLException {
		if(getApplicationConfiguration().containsKey("base.before")) {
			SQLFileProcessor fileProcessor = new SQLFileProcessor(this.sourceConnection);
			fileProcessor.processFile(Paths.get(getApplicationConfiguration().get("base.before")));
		}
		for (TableDefinition tableDefinition : this.schemaDefinition.getTableDefinitions()) {
			T processor = this.preparer.apply(tableDefinition);
			String tableName = tableDefinition.tableName();
			if((this.includeTables.isEmpty() || this.includeTables.contains(tableName)) && !this.excludeTables.contains(tableDefinition.tableName())) {
				this.processor.executeCommand(processor);
			}
		}
		if(getApplicationConfiguration().containsKey("base.after")) {
			SQLFileProcessor fileProcessor = new SQLFileProcessor(this.sourceConnection);
			fileProcessor.processFile(Paths.get(getApplicationConfiguration().get("base.after")));
		}

	}
	
	public Connection getConnection(String prefix) throws MissingPropertyException, IOException, SQLException {
		final String base=prefix+".%s";
		final String url = getProp(String.format(base,"jdbcURL"));
		final String driver = getProp(String.format(base,"jdbcDriver"));
		
		Path driverFile = Paths.get(driver);
		URL driverFileURL = driverFile.toUri().toURL();
		final String driverClassName = getProp(String.format(base,"jdbcDriverClass"));
		final String user = getProp(String.format(base,"jdbcUser"));
		final String password = getProp(String.format(base,"jdbcPassword"));
		
		try  {
			ClassLoader classLoader = new JarClassloader(driverFileURL);
			LOGGER.info(String.format("Establishing connection to %s",url));
			Class<?> driverClass = Class.forName(driverClassName,true,classLoader);
			Constructor<?> constructor = driverClass.getConstructor();
			Driver driverInstance = (Driver) constructor.newInstance();
			Properties connectionProps = new Properties();
			connectionProps.put("user", user);
			connectionProps.put("password", password);
			Connection connection = driverInstance.connect(url, connectionProps);
			LOGGER.info(String.format("Connection established to %s",url));
			return connection;
		} catch(Exception e) {
			throw new DatabaseCopyRuntimeException(e);
		}
	}
	
	private String getProp(String prop) throws MissingPropertyException {
		if(!getApplicationConfiguration().containsKey(prop)) {
			throw new MissingPropertyException(String.format("The following property is missing in %s"));
		}
		return getApplicationConfiguration().get(prop);
	}

	public Connection getSourceConnection() {
		return sourceConnection;
	}
	
	public Map<String,String> getApplicationConfiguration() {
		return this.applicationConfiguration;
	}

	public void setProcessor(CommandTableStepProcessor<T> processor) {
		this.processor = processor;
	}

	public void setPreparer(Function<TableDefinition, T> preparer) {
		this.preparer = preparer;
	}

	private static final class JarClassloader extends ClassLoader {
		private final ZipFile zipFile;
	
		private JarClassloader(URL driverFileURL) throws IOException, URISyntaxException {
			this.zipFile = new ZipFile(new File(driverFileURL.toURI()));
		}
	
		@Override
		protected Class<?> findClass(String name) throws ClassNotFoundException {
			byte[] inFile = findInFile(name);
			if(inFile != null) {
				return defineClass(name, inFile, 0, inFile.length);
			} else {
				return super.findClass(name);
			}
		}
	
		private byte[] findInFile(String name) {
			name=name.replace('.',File.separatorChar)+".class";
			try {
				Enumeration<? extends ZipEntry> jarEntries = this.zipFile.entries();
				while(jarEntries.hasMoreElements()) {
					ZipEntry jarEntry = jarEntries.nextElement();
					String entryName = jarEntry.getName();
					if(entryName.equals(name)) {
						InputStream inputStream = this.zipFile.getInputStream(jarEntry);
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						int b;
						while((b=inputStream.read())!=-1) {
							bos.write(new byte[] {(byte)b});
						}
						return bos.toByteArray();
					}
				}
				return null;
			} catch (IOException e) {
				return null;
			}
		}
	}

	private interface LoadProperties {
		public void loadProperties(Properties props) throws IOException;
	}
}
