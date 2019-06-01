package tinydb.exec.consts;

public class IntConstant implements Constant {
	private Integer val;

	public IntConstant(int n) {
		val = new Integer(n);
	}

	public Object value() {
		return val;
	}

	@SuppressWarnings("finally")
	public boolean equals(Object obj) {
		IntConstant ic = null;
		try {
			ic = (IntConstant) obj;
		} catch (java.lang.ClassCastException e) {
			ic = new IntConstant(((Long) ((LongConstant) obj).value()).intValue());
		} finally {
			return ic != null && val.equals(ic.val);
		}
	}

	@SuppressWarnings("finally")
	public int compareTo(Constant c) {
		IntConstant ic = null;
		try {
			ic = (IntConstant) c;
		} catch (java.lang.ClassCastException e) {
			ic = new IntConstant(((Long) ((LongConstant) c).value()).intValue());
		} finally {
			return val.compareTo(ic.val);
		}
	}
}
