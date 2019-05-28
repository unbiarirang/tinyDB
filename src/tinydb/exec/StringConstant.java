package tinydb.exec;

public class StringConstant implements Constant {
	private String val;

	public StringConstant(String s) {
		val = s;
	}

	public String asJavaVal() {
		return val;
	}

	public boolean equals(Object obj) {
		StringConstant sc = (StringConstant) obj;
		return sc != null && val.equals(sc.val);
	}

	public int compareTo(Constant c) {
		StringConstant sc = (StringConstant) c;
		return val.compareTo(sc.val);
	}

	public int hashCode() {
		return val.hashCode();
	}

	public String toString() {
		return val;
	}
}
