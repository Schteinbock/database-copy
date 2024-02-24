package de.us.dbcopy.exception;

public class MissingPropertyException extends DatabaseCopyException {

	private static final long serialVersionUID = 1L;
	
	public MissingPropertyException() {
		super();
	}
	public MissingPropertyException(String message) {
		super(message);
	}
}
