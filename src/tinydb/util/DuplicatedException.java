package tinydb.util;

@SuppressWarnings("serial")
public class DuplicatedException extends RuntimeException {
	public DuplicatedException() {
	}

	public DuplicatedException(String msg) {
		super(msg);
	}
}
