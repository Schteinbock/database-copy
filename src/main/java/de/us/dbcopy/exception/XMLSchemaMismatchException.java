package de.us.dbcopy.exception;

public class XMLSchemaMismatchException extends DatabaseCopyException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public XMLSchemaMismatchException() {
	}
	
	public XMLSchemaMismatchException(String message) {
		super(message);
	}
	
	public XMLSchemaMismatchException(Throwable cause,String message) {
		super(cause,message);
	}

}
