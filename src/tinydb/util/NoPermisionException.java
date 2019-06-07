package tinydb.util;

@SuppressWarnings("serial")
public class NoPermisionException extends RuntimeException {
	public NoPermisionException() {
	}

	public NoPermisionException(String msg) {
		super(msg);
	}
}
