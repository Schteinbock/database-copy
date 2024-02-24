package de.us.dbcopy.exception;

public class DatabaseSchemaMissmatchException extends DatabaseCopyException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public DatabaseSchemaMissmatchException() {
	}
	
	public DatabaseSchemaMissmatchException(String message) {
		super(message);
	}
	
	public DatabaseSchemaMissmatchException(Throwable cause,String message) {
		super(cause,message);
	}

}
