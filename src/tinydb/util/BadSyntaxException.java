package tinydb.util;

@SuppressWarnings("serial")
public class BadSyntaxException extends RuntimeException {
	public BadSyntaxException() {
	}

	public BadSyntaxException(String msg) {
		super(msg);
	}
}