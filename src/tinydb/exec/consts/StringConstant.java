package tinydb.exec.consts;

public class StringConstant implements Constant {
	private String val;

	public StringConstant(String s) {
		val = s;
	}

	public String value() {
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
}
