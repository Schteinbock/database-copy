package de.us.dbcopy.exception;

public class DatabaseCopyRuntimeException extends RuntimeException {
	private static final long serialVersionUID = -177779542812829465L;

	public DatabaseCopyRuntimeException() {
		super();
	}
	
	public DatabaseCopyRuntimeException(String message) {
		super(message);
	}
	
	public DatabaseCopyRuntimeException(Throwable cause) {
		super(cause);
	}
	
	public DatabaseCopyRuntimeException(String message,Throwable cause) {
		super(message,cause);
	}
}
