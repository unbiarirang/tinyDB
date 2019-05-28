package tinydb.exec;

// Wrap Java string as database constants.
public class DoubleConstant implements Constant {
	private Double val;

	public DoubleConstant(double s) {
		val = s;
	}

	public Object asJavaVal() {
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

	public int hashCode() {
		return val.hashCode();
	}

	public String toString() {
		return val.toString();
	}
}
