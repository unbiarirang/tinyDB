package tinydb.parse;

@SuppressWarnings("serial")
public class BadSyntaxException extends RuntimeException {
	public BadSyntaxException() {
	}

	public BadSyntaxException(String msg) {
		super(msg);
	}
}
