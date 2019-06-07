package tinydb.exec.consts;

import static tinydb.consts.Types.STRING;

public class StringConstant implements Constant {
	private String val;
	private boolean isNull = false;

	public StringConstant(String s) {
		val = s;
	}
	public StringConstant(String s, boolean isNull) {
		val = s;
		this.isNull = isNull;
	}

	public String value() {
		return val;
	}
	
	public int type() {
		return STRING;
	}

	public boolean isNull() {
		return isNull;
	}

	public boolean equals(Object obj) {
		StringConstant sc = (StringConstant) obj;
		return sc != null && val.equals(sc.val);
	}

	public int compareTo(Constant c) {
		StringConstant sc = (StringConstant) c;
		return val.compareTo(sc.val);
	}
	
	public String toString() {
		return val.toString();
	}
}
