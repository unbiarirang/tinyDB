package tinydb.exec.consts;

// Wrap Java value as database constants.
public class DoubleConstant implements Constant {
	private Double val;

	public DoubleConstant(double s) {
		val = s;
	}

	public Object value() {
		return val;
	}

	public boolean equals(Object obj) {
		DoubleConstant sc = (DoubleConstant) obj;
		return sc != null && val.equals(sc.val);
	}

	public int compareTo(Constant c) {
		DoubleConstant sc = (DoubleConstant) c;
		return val.compareTo(sc.val);
	}
}
