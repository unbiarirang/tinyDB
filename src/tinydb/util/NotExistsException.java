package tinydb.util;

@SuppressWarnings("serial")
public class NotExistsException extends RuntimeException {
	public NotExistsException() {
	}

	public NotExistsException(String msg) {
		super(msg);
	}
}
