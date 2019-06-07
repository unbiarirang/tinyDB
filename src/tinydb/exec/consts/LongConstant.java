package tinydb.exec.consts;

import static tinydb.consts.Types.LONG;

public class LongConstant implements Constant {
	private Long val;
	private boolean isNull = false;

	public LongConstant(long s) {
		val = s;
	}
	public LongConstant(long s, boolean isNull) {
		val = s;
		this.isNull = isNull;
	}

	public Object value() {
		return val;
	}
	
	public int type() {
		return LONG;
	}
	
	public boolean isNull() {
		return isNull;
	}

	public boolean equals(Object obj) {
		LongConstant sc = (LongConstant) obj;
		return sc != null && val.equals(sc.val);
	}

	public int compareTo(Constant c) {
		LongConstant sc = (LongConstant) c;
		return val.compareTo(sc.val);
	}
	
	public String toString() {
		return val.toString();
	}
}
