package tinydb.exec.consts;

import static tinydb.consts.Types.DOUBLE;

// Wrap Java value as database constants.
public class DoubleConstant implements Constant {
	private Double val;
	private boolean isNull = false;

	public DoubleConstant(double s) {
		val = s;
	}
	public DoubleConstant(double s, boolean isNull) {
		val = s;
		this.isNull = isNull;
	}

	public Object value() {
		return val;
	}
	
	public int type() {
		return DOUBLE;
	}
	
	public boolean isNull() {
		return isNull;
	}

	public boolean equals(Object obj) {
		DoubleConstant sc = (DoubleConstant) obj;
		return sc != null && val.equals(sc.val);
	}

	public int compareTo(Constant c) {
		DoubleConstant sc = (DoubleConstant) c;
		return val.compareTo(sc.val);
	}
	
	public String toString() {
		return val.toString();
	}
}
