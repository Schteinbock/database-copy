package de.us.dbcopy.exception;

public class DatabaseCopyException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public DatabaseCopyException() {
	}

	public DatabaseCopyException(Throwable cause) {
		super(cause);
	}
	
	public DatabaseCopyException(String message) {
		super(message);
	}
	
	public DatabaseCopyException(Throwable cause,String message) {
		super(message,cause);
	}
}
