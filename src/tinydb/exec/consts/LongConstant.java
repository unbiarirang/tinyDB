package tinydb.exec.consts;

public class LongConstant implements Constant {
	private Long val;

	public LongConstant(long s) {
		val = s;
	}

	public Object value() {
		return val;
	}

	public boolean equals(Object obj) {
		LongConstant sc = (LongConstant) obj;
		return sc != null && val.equals(sc.val);
	}

	public int compareTo(Constant c) {
		LongConstant sc = (LongConstant) c;
		return val.compareTo(sc.val);
	}
}
