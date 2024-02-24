# database-copy
## About this project

This is a CLI tool for transfering data table or schemawise between databases. It's source is Java 8 compatible and designed runs on any JVM (Java 8 or higher). It is generally designed to be efficient and versatile. This tool uses JDBC and tries to be as independent from the underlying database technology as possible. Currently unfortuneatly only two database technologies are supported natively. However even if your database technology is not natively supported, there are good chances that it can be made compatible with few effort.
## Natively supported databases:
* DB2
* H2

## Limitations
* Use on your own risk
* No JDK shipped
* No drivers shipped
* The datatype "ARRAY" is not supported
## Commands
### transfer
Transfers tables and schemas between two connections.
### export
Exports tables to the given directory in the .xml.gz format
### import
Imports tables from the given directory from .xml.gz format

## Configurationfile
On each execution you are required to specify one or more java property files to read the application configuration from.
### Generic Arguments
#### base.connection
The following arguments are always required:
base.connection.jdbcURL - The URL to connect to
base.connection.jdbcUser - The database user used for data
base.connection.jdbcPasswordd - The database users password used for the connection
base.connection.jdbcDriverClass - The class in the JAR of the JDBC driver required to establish the connection
base.connection.jdbcDriver - The location of the JAR to the JDBC driver, on Linux no Tilde as homepath is possible
#### Usage
* transfer: The source connection used to transfer data from (base.connection.jdbcUser must have at least the "SELECT" privilege)
* export: The connection used to export tables from (base.connection.jdbcUser must have at least the "SELECT" privilege
* import: The connection used to import table into (base.connection.jdbcUser must have at least the "INSERT" privilege. The database must already contain the tables exported into.
#### base.commitcount
Integer value to process stuff after n rows have been processed.
#### base.exclude
Comma separated list with tables to skip.
#### base.include
Comma separated list of tables. If specified only the tables specified will be exported, imported or transfered.
#### Usage
* transfer: After n rows have been processed, execute INSERT and clear batch
* import: After n rows have been read, execute INSERT and clear batch
* export: After n rows print n to inform the user that the process is still running
### Specific arguments
Arguments used by specific commands only.
#### import.sourcedir
Required for the **import** command. Source directory for the command to read the import data from. You must have read rights for the directory.
#### export.targetdir
Required for the **export**. Defines where to export the data to. You must have write rights for the directory
### How to include your own database technology
Append the following arguments to make your database technology compatible with this software. Replace **?** with the product name of your database technology:
#### ?.gettables.statement
A statement compatible to receive a list of tables that will be used. Use the placeholder "{0}" to put the schema (if applicable)
Example for a value: SHOW TABLES FOR {0}
#### ?.gettables.tablenamearg
Where to get the table name on executing the "gettables.statement". Either an integer value or a named value.
Example: TBNAME
#### ?.getcolumns.statement
A statement compatible to receive a list of columns and their database types. Use the placeholder {0} for the table name and {1} for the schema name (optional).
Example: SHOW COLUMNS FOR {0} FROM {1}
#### ?.getcolumns.columnnamearg
The return column name or index of "?.getcolumns.statement" what name the column has in the table.
#### ?.getcolumns.columntypearg
The return column name or index of "?.getcolumns.statement" what database type the column has
